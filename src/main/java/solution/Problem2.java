package solution;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dto.ProfitableStock;
import dto.SumAndCount;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import dto.StockData;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

/**
 * Calculating the stock out of the four stocks giving maximum profit
 * (average closing price - average opening price) in a 5-minute sliding window
 * for the last 10 minutes.
 */
public class Problem2 implements Serializable, Problem {
    private String broker;
    private Set<String> topics;
    private String consumerGroupId = "kafkaConsumer10009";
    private String profitableStockName;
    private double maxStockPrice;


    public Problem2(String broker, String topics) {
        this.broker = broker;
        this.topics = new HashSet<>(Arrays.asList(topics.split(",")));
    }

    public void consumeDataAndAnalyse() throws InterruptedException {
        //Creating spark config while disabling caching of RDDs to prevent
        //concurrent modification for computing other graphs.
        SparkConf sparkConf = new SparkConf().setAppName("StockDataConsumer").setMaster("local[*]").set("spark.streaming.kafka.consumer.cache.enabled", "false");

        //Creating streaming context having micro batches duration of 1 minute.
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.minutes(1));
        streamingContext.sparkContext().setLogLevel("WARN");

        //Setting configuration for connection to kafka broker using the properties
        //provided by user
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);

        //Connecting to the kafka broker to receive consumer records from the
        //topic provided by user
        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));

        //Conversion of records obtained from kafka into StockData
        //and using this data to set ProfitableStock (set on per record(JSON) basis)
        JavaPairDStream<String, ProfitableStock> stockData = messages
                .mapToPair(new PairFunction<ConsumerRecord<String, String>, String, ProfitableStock>() {
                    private static final long serialVersionUID = 1L;

                    public Tuple2<String, ProfitableStock> call(ConsumerRecord<String, String> stockInfo) throws Exception {

                        //Creating mapper object
                        ObjectMapper mapper = new ObjectMapper();

                        // Defining the return type
                        TypeReference<StockData> mapType = new TypeReference<StockData>() {
                        };

                        // Parsing the JSON String into StockData
                        StockData stock = mapper.readValue(stockInfo.value(), mapType);

                        //Setting data into ProfitableStock from the obtained StockData
                        ProfitableStock profitableStock = new ProfitableStock(stock.getPriceData().getClose(),
                                stock.getPriceData().getOpen(), 1);

                        //Returning <Stock_Symbol,ProfitableStock>
                        return new Tuple2<>(stock.getSymbol(), profitableStock);
                    }
                });

        //Aggregating the records based on key(Stock_Symbol).
        //All the records for a particular key will be added and
        //finally, we'll be having the aggregated value for all the 4 Stocks.
        //All this will happen with a window size of 10 minutes and sliding interval of 5 minutes.
        JavaPairDStream<String, ProfitableStock> result = stockData.reduceByKeyAndWindow(
                new Function2<ProfitableStock, ProfitableStock, ProfitableStock>() {

                    private static final long serialVersionUID = 76761212;

                    public ProfitableStock call(ProfitableStock stock1, ProfitableStock stock2) throws Exception {

                        //The parameter closing price from first object will be aggregated with the
                        //with the closing price of second object
                        stock1.setClosingPriceSum(stock1.getClosingPriceSum() + stock2.getClosingPriceSum());

                        //The parameter opening price from first object will be aggregated with the
                        //with the opening price of second object
                        stock1.setOpeningPriceSum(stock1.getOpeningPriceSum() + stock2.getOpeningPriceSum());

                        //All the counts will be added
                        stock1.setCount(stock1.getCount() + stock2.getCount());

                        //Returning the updated data for that particular key
                        return stock1;
                    }
                }, Durations.minutes(10), Durations.minutes(5));

        //Printing the Batch data
        messages.map(record -> record.value()).print();

        //Printing (average closing price - average opening price) for each Stock
        result.print();

        //Finding the most profitable stock and printing
        result.foreachRDD(rdd -> {
            for (Tuple2<String, ProfitableStock> stock : rdd.collect()) {
                if (maxStockPrice < stock._2.getProfit()) {
                    maxStockPrice = stock._2.getProfit();
                    profitableStockName = stock._1;
                }
            }
            System.out.println("Most Profitable stock: " + profitableStockName);
        });

        //Printing the result to a file
        result.foreachRDD(new VoidFunction<JavaPairRDD<String, ProfitableStock>>() {
            @Override
            public void call(JavaPairRDD<String, ProfitableStock> closingPriceForStocks) throws Exception {
                closingPriceForStocks.coalesce(1).saveAsTextFile("sparkResult-" + System.currentTimeMillis());
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.stop();
    }
}
