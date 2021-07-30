package homework.fraudio

import org.apache.spark.util.LongAccumulator

object TestUtils {

  val emptyConvert: RateConversionFunction = (date, currencies) => Map.empty
  val inverseConvert: RateConversionFunction = (date, currencies) => currencies.map(cur => ((date, cur), checkUsd(cur, -1.0))).toMap
  val doubleConvert: RateConversionFunction = (date, currencies) => currencies.map(cur => ((date, cur), checkUsd(cur, 2.0))).toMap

  def checkUsd(currency: String, rate: ConversionRate): ConversionRate = if (currency == "USD") 1.0 else rate

  def countCallsToConverter(counter: LongAccumulator, converterFunc: RateConversionFunction): RateConversionFunction = (date, currencies) => {
    counter.add(1)
    converterFunc(date, currencies)
  }
}
