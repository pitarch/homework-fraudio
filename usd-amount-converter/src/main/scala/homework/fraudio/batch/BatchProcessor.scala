package homework.fraudio.batch

import homework.fraudio._
import org.apache.spark.sql.functions.{broadcast, col, collect_set, to_date}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql._

object BatchProcessor {

  implicit val datedConversionRateEncoder: Encoder[DatedConversionRate] = Encoders.product[DatedConversionRate]

  /**
   * convert the transaction's amount by the USD conversion rate of the underlying currency for underlying date. In case,
   * there no conversion, the resulting converted amount will contain 'null'.
   *
   * @param transactions
   * @param provideRateConversion
   * @return
   */
  def convert(transactions: Dataset[Transaction], provideRateConversion: RateConversionFunction): Dataset[ConvertedTransaction] = {

    // cleanse and add column 'date'
    val transactionWithDateDf = transactions
      .filter(col("currency").rlike("^[A-Z]{3}$"))
      .withColumn("date", to_date(col("timestamp").cast(TimestampType), "yyyy-MM-dd"))
      .cache()

    // fold to aggregate currencies by date
    val currenciesGroupedByDateDf = transactionWithDateDf
      .select("date", "currency")
      .groupBy("date")
      .agg(collect_set("currency").alias("currencies"))

    // enrich the historical rate table with conversation rates
    val historicalRateDf = enrichWithFxRateFactor(currenciesGroupedByDateDf, provideRateConversion).cache()
    val histRateBroadcast = broadcast(historicalRateDf)

    // join the historical conversation rate dimension table the transaction table
    val df = transactionWithDateDf.join(histRateBroadcast,
      transactionWithDateDf("date") === histRateBroadcast("date") &&
        transactionWithDateDf("currency") === histRateBroadcast("currency"), "left")

    val convertedTransactionDf = df
      .withColumn("amount", col("rate") * col("amount"))
      .drop("date", "rate", "currency")
      .as[ConvertedTransaction](Encoders.product[ConvertedTransaction])

    transactionWithDateDf.unpersist()
    historicalRateDf.unpersist()

    convertedTransactionDf
  }

  private def enrichWithFxRateFactor(currenciesGroupedByDateDf: DataFrame, provideRateConversion: RateConversionFunction): Dataset[DatedConversionRate] =
    currenciesGroupedByDateDf.mapPartitions(processPartitionOfAggCurrenciesByDate(_, provideRateConversion))


  /**
   *
   * @param rows                  list of rows in which each row denotes the aggregated set of currencies used in some date
   * @param provideRateConversion function that returns the conversation rates in some specific date
   * @return an iterator of all conversion rates
   */
  def processPartitionOfAggCurrenciesByDate(rows: Iterator[Row], provideRateConversion: RateConversionFunction): Iterator[DatedConversionRate] = {
    val rates = for {
      row <- rows
      date = row.getDate(0).toLocalDate
      currencies = row.getSeq[Currency](1).toSet
      rates = provideRateConversion(date, currencies)
      currency <- currencies
      rate <- rates.get((date, currency))
    } yield DatedConversionRate(date, currency, rate)

    rates
  }
}
