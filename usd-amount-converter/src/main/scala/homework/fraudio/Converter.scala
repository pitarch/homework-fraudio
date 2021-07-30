package homework.fraudio

import homework.fraudio.batch.BatchProcessor
import homework.fraudio.caching.Jvm.createJvmCache
import homework.fraudio.caching.Redis.RedisConfig
import homework.fraudio.caching.{addJvmAndRedisCaching, addJvmCaching}
import homework.fraudio.conversion.CurrencyLayerApi
import homework.fraudio.realtime.RealTimeProcessor
import org.apache.spark.sql.Dataset

trait Converter {

  def convertInRealTime(transaction: Transaction): ConvertedTransaction

  def convertInBatch(transactions: Dataset[Transaction]): Dataset[ConvertedTransaction]
}


object Converter {

  def usingCurrencyLayerApi(apiKey: String): Converter = Converter(CurrencyLayerApi.live(apiKey).getRates)

  def apply(convertFunc: RateConversionFunction): Converter = new Converter() {

    val realTimeProcessor = new RealTimeProcessor(convertFunc)

    override def convertInRealTime(transaction: Transaction): ConvertedTransaction = realTimeProcessor.convert(transaction)

    override def convertInBatch(transactions: Dataset[Transaction]): Dataset[ConvertedTransaction] = BatchProcessor.convert(transactions, convertFunc)
  }

  def usingCurrencyLayerApiWithJvmCache(apiKey: String): Converter =
    Converter(addJvmCaching(CurrencyLayerApi.live(apiKey).getRates, createJvmCache))

  def usingCurrencyLayerApiWithJvmCacheAndRedis(apiKey: String, redisConfig: RedisConfig): Converter =
    Converter(addJvmAndRedisCaching(CurrencyLayerApi.live(apiKey).getRates, createJvmCache, redisConfig))
}

