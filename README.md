# REAL-TIME AND BATCH RATE CONVERSION

This project contains a component for converting the amount of transactions being different currencies into USD in 
real-time and batch execution environments.

 
## Tooling

This is a list of the required tools to build and run this application:

- [JDK 1.8](https://adoptopenjdk.net/?variant=openjdk8&jvmVariant=hotspot)
- [sbt v1.5](https://www.scala-sbt.org/)
- [scala 2.12.13](https://scala-lang.org/download/2.12.13.html)
- [Apache Spark 3.0.2](https://spark.apache.org/docs/3.0.2/)
- HTTP Client [sttp v3.3.12](https://sttp.softwaremill.com/en/v3.3.12/)

## Assumptions

- real-time conversions use caching to reduce calling the REST API
- the redis caching service (see `homework.fraudio.caching.Redis`) has not implemented and only delegates the conversation to the underlying converter
- The live code uses the `https://currencylayer.com/historical` endpoint.
- The HTTP client is synchronous for simplicity
- When batch processing is used, the transaction dataset is reduced to aggregate currencies by date. This allows to make a call per day.
- If there is no rate conversation for some day and currency, the converted amount will be null. (or None).
- transactions are ignored when its currency does not comply the ISO 4217 Alpha codes.


## Testing

Check out the class `ConverterIntegrationTests` to use the usage of the component in a real-time and in
a batch environment.

Run 

```
sbt test
```

```
 RealTimeProcessorTests:
 - should convert in real-time when there is rate conversion
 - should do not convert transaction in real-time when there is no rate conversion
 - should retrieve the conversation rate from the cache
 BatchProcessorTests:
 - the historical rate conversation table should contain a rate for each pair (date, currency) that
 - should convert the transaction amount when there exists a conversation rate
 - different rates will produce different amounts when converted
 - transactionDs with no conversion rate shows 'None' in their amount
 - should call currencyLayer once per date
 CurrencyLayerRestApiTests:
 - should parse the response from currencylayer.con containing the historical rates
 CurrencyLayerApiIntegrationTests:
 - should get the historical rates for USD currency in some day from the rest api of CurrencyLayer.com
 ConverterIntegrationTests:
 - should convert real-time transaction
 - should convert real-time transaction using the cache
 - should convert transaction in batch
```


## Code

This component is represented by the trait `Converter` that offers an api for the realtime
conversions and another api for batch conversations.

```scala

  def convertInRealTime(transaction: Transaction): ConvertedTransaction

  def convertInBatch(transactions: Dataset[Transaction]): Dataset[ConvertedTransaction]
```

The result of the conversion is a `ConvertedTransaction` that is similar to the original transaction but
with no currency and its amount representing the USD conversion for that day.

Both methods use the same function to retrieve the FX rate conversation. This function is defined as:

```scala
type RateConversionFunction = (LocalDate, Set[Currency]) => Map[ConversionRateKey, ConversionRate]
```

The `CurrencyLayerApi` implements this function. The implementation consists of calling the endpoint `/historical` of
`http://currencylayer.com`.

```scala
class CurrencyLayerApi(apiKey: String, url: String = "http://api.currencylayer.com/historical", source: String = "USD") {

  def getRates(date: LocalDate, currencies: Set[Currency]): Map[ConversionRateKey, ConversionRate] = ???
}
```


In order to reduce calls to this layer a couple of caching techniques will be applied in the real-time environment:

- `Converter.usingCurrencyLayerApiWithJvmCache` creates a converter using the JVM cache
- `Converter.usingCurrencyLayerApiWithJvmCacheAndRedis` creates a converter using the JVM cache and a redis cache.

