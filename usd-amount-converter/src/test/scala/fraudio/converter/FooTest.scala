package fraudio.converter

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}

import java.time.LocalDate

class FooTest extends IntegrationTestBase {

  val path = "/Users/pitarch/projects/incubating_/fraudio/usd-amount-converter/src/test/resources/transactions.csv"

  def readTestCsv: DataFrame = spark.read
    .option("header", value = true)
    .option("mode", "DROPMALFORMED") // ignore malformed records
    .schema(inputTransactionSchema)
    .csv(path)
    .filter(col("currency").rlike("^[A-Z]{3}$"))


  test("cre") {
    import spark.implicits._
    val convert: (LocalDate, Set[Currency]) => Map[Currency, MaybeConversionRate] = (_, currencyCodes) => currencyCodes.map { code => (code, Some(-1.0)) }.toMap
    val df1 = readTestCsv.sample(0.1)
    val df2 = BatchProcessing.cleanse(df1)
    val df3 = BatchProcessing.enrichWithDate(df2.toDF(), spark).cache()
    val df4 = BatchProcessing.buildCurrenciesGroupedByDate(df3)
    val histRatesDf = BatchProcessing.enrichWithFxRateFactor(df4, convert, spark)
    val histRates = histRatesDf
      .map(rate => ((rate.date, rate.rate), rate.currency))
      .collect()
      .toMap

    val broadcastedHisRates = spark.sparkContext.broadcast(histRatesDf)
  }


  test("rename multiple columns") {

    val oldNames = Seq("transaction_id", "customer_id", "merchant_id", "card_id")
    val newNames = Seq("transactionId", "customerId", "merchantId", "cardId")


  }
}
