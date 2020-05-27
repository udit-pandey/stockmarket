# stockmarket
Analysis of stock market prices in real-time to get insights and help make informed decisions related to the stocks.

Real-time global equity data has been provided. The data contains the following information:

symbol - id of the stock <br />
timestamp - time at which we are getting the data
open - the price at which the particular stock opens in the time period
high - highest price of the stock during the time period
low - lowest price of the stock during the time period
close - the price at which the particular stock closes in the time period
volume - indicates the total number of transactions involving the given stock in the time period

This data is hosted on a centralized Kafka server. In the Kafka server, we are getting real-time data for the 4 cryptocurrencies,  BTC (Bitcoin), ETH (Ethereum), LTC (Litecoin) and XRP using a JAVA API of CryptoCompare. This API fetches the data for these 4 cryptocurrencies every minute and writes it into the above mentioned Kafka topic.

 
Problem Statement
Perform the below-mentioned analysis. The results of the analyses should be written in an output file. These results will act as insights to make informed decisions related to the stocks.
Fetch data every minute relating to the following four cryptocurrencies from the Kafka topic. 
BTC (Bitcoin)
ETH (Ethereum)
LTC (Litecoin)
XRP
 
1. Calculate the simple moving average closing price of the four stocks in a 5-minute sliding window for the last 10 minutes.  Closing prices are used mostly by the traders and investors as it reflects the price at which the market finally settles down. The SMA (Simple Moving Average) is a parameter used to find the average stock price over a certain period based on a set of parameters. 

2. Find the stock out of the four stocks giving maximum profit (average closing price - average opening price) in a 5-minute sliding window for the last 10 minutes. 

3. Calculate the trading volume(total traded volume) of the four stocks every 10 minutes and decide which stock to purchase out of the four stocks. Take the absolute value of the volume. Volume plays a very important role in technical analysis as it helps us to confirm trends and patterns. You can think of volumes as a means to gain insights into how other participants perceive the market. Volumes are an indicator of how many stocks are bought and sold over a given period of time. Higher the volume, more likely the stock will be bought. 
