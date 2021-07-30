package homework.fraudio

import homework.fraudio.caching.Jvm.{createJvmCache, getRatesCachedInJvmOrDelegate}
import homework.fraudio.caching.Redis.{RedisConfig, getRatesFromRedisOrDelegate}

import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap

package object caching {

  /**
   * Add JVM caching, then Redis caching and, finally, delegate the rate conversion if it is not found in any cache
   *
   * @param delegate
   * @param jvmCache
   * @param config
   * @return
   */
  def addJvmAndRedisCaching(delegate: RateConversionFunction, jvmCache: ConcurrentHashMap[ConversionRateKey, ConversionRate] = createJvmCache, config: RedisConfig = RedisConfig()): RateConversionFunction = {
    val accessRedisOrDelegate = getRatesFromRedisOrDelegate(config)(delegate) _
    addJvmCaching(accessRedisOrDelegate, jvmCache)
  }

  /**
   * Add JVM caching before delegating the rate conversation
   *
   * @param delegate
   * @param jvmCache
   * @return
   */
  def addJvmCaching(delegate: RateConversionFunction, jvmCache: ConcurrentHashMap[ConversionRateKey, ConversionRate]): RateConversionFunction =
    getRatesCachedInJvmOrDelegate(jvmCache)(delegate)


  /**
   * Add Redis caching before delegating the rate conversation
   *
   * @param delegate
   * @param config
   * @return
   */
  def addRedisCaching(delegate: RateConversionFunction, config: RedisConfig): (LocalDate, Set[Currency]) => Map[(LocalDate, Currency), ConversionRate] =
    getRatesFromRedisOrDelegate(config)(delegate)

  object Jvm {

    def createJvmCache = new ConcurrentHashMap[ConversionRateKey, ConversionRate](1000)

    def getRatesCachedInJvmOrDelegate(cache: ConcurrentHashMap[ConversionRateKey, ConversionRate])(innerConverterFunc: RateConversionFunction)(date: LocalDate, currencies: Set[Currency]): Map[ConversionRateKey, ConversionRate] = {

      val (cachedKeys, uncachedKeys) = Iterator.continually(date)
        .zip(currencies.iterator)
        .partition(cache.containsKey)

      val cachedRates = cachedKeys.map(key => (key, cache.get(key))).toMap
      val newRates = if (uncachedKeys.nonEmpty) innerConverterFunc(date, uncachedKeys.map(_._2).toSet) else Map.empty[ConversionRateKey, ConversionRate]
      newRates.foreach { case ((date, currency), rate) => cache.put((date, currency), rate) }
      newRates ++ cachedRates
    }
  }

  object Redis {

    def getRatesFromRedisOrDelegate(config: RedisConfig = RedisConfig())(innerConverterFunc: RateConversionFunction)(date: LocalDate, currencies: Set[Currency]): Map[ConversionRateKey, ConversionRate] = {
      // TODO call redis to get the conversion rate. Not Implemented
      innerConverterFunc(date, currencies)
    }

    case class RedisConfig(host: String = "localhost", port: Int = 6379)
  }
}
