package solution;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import dto.StockData;
import dto.SumAndCount;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

/**
 * Calculates the simple moving average closing price of the four stocks in a
 * 5-minute sliding window for the last 10 minutes
 */
public class Problem1 implements Serializable, Problem {
    private String broker;
    private Set<String> topics;
    private String consumerGroupId = "kafkaConsumer10008";

    public Problem1(String broker, String topics) {
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
        //and using this data to set SumAndCount (set on per record(JSON) basis)
        JavaPairDStream<String, SumAndCount> stockData = messages
                .mapToPair(new PairFunction<ConsumerRecord<String, String>, String, SumAndCount>() {
                    private static final long serialVersionUID = 1L;

                    public Tuple2<String, SumAndCount> call(ConsumerRecord<String, String> stockInfo) throws Exception {

                        //Creating mapper object
                        ObjectMapper mapper = new ObjectMapper();

                        // Defining the return type
                        TypeReference<StockData> mapType = new TypeReference<StockData>() {
                        };

                        // Parsing the JSON String into StockData object
                        StockData stock = mapper.readValue(stockInfo.value(), mapType);

                        //Setting data into SumAndCount from the obtained StockData
                        SumAndCount sumAndCountForStock = new SumAndCount();
                        sumAndCountForStock.setSum(stock.getPriceData().getClose());
                        sumAndCountForStock.setCount(1);

                        //Returning <Stock_Symbol,SumAndCount>
                        return new Tuple2<>(stock.getSymbol(), sumAndCountForStock);
                    }
                });

        //Aggregating the records based on key(Stock_Symbol).
        //All the records for a particular key will be added and
        //finally, we'll be having the aggregated value for all the 4 Stocks.
        //All this will happen with a window size of 10 minutes and sliding interval of 5 minutes.
        JavaPairDStream<String, SumAndCount> result = stockData.reduceByKeyAndWindow(
                new Function2<SumAndCount, SumAndCount, SumAndCount>() {

                    private static final long serialVersionUID = 76761212;

                    public SumAndCount call(SumAndCount stock1, SumAndCount stock2) throws Exception {
                        //Adding the sum for same stock symbol
                        stock1.setSum(stock1.getSum() + stock2.getSum());

                        //Adding the count
                        stock1.setCount(stock1.getCount() + stock2.getCount());

                        //Returning the updated data for that particular key
                        return stock1;
                    }
                }, Durations.minutes(10), Durations.minutes(5));

        //Printing the Batch data
        messages.map(record -> record.value()).print();

        //Printing the Simple Moving Average
        result.print();

        //Printing the result to a file
        result.foreachRDD(new VoidFunction<JavaPairRDD<String, SumAndCount>>() {
            @Override
            public void call(JavaPairRDD<String, SumAndCount> closingPriceForStocks) throws Exception {
                closingPriceForStocks.coalesce(1).saveAsTextFile("sparkResult-" + new Timestamp(System.currentTimeMillis()));
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.stop();
    }
}
