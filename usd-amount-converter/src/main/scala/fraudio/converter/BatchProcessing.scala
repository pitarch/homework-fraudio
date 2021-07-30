package fraudio.converter

import org.apache.spark.sql.functions.{col, collect_set, to_date}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql._

import java.time.LocalDate

object BatchProcessing {

  def createHistoricalRates(df: DataFrame, convert: (LocalDate, Set[Currency]) => Map[Currency, MaybeConversionRate], sparkSession: SparkSession): Dataset[DatedConversionRate] = {

    val cleansed = cleanse(df)
    val enrichedWithDate = enrichWithDate(cleansed.toDF(), sparkSession)
    val currenciesGroupedByDate = buildCurrenciesGroupedByDate(enrichedWithDate)
    val enrichedWithRateFactor = enrichWithFxRateFactor(currenciesGroupedByDate, convert, sparkSession)

    enrichedWithRateFactor
  }

  def cleanse(df: DataFrame): Dataset[Transaction] = {
    implicit val transactionEncoder: Encoder[Transaction] = Encoders.product[Transaction]

    df
      // remove records with currency field that does not comply ISO 4217 Alpha currencies
      .filter(col("currency").rlike("^[A-Z]{3}$"))
      .withColumnRenamed("transaction_id", "transactionId")
      .withColumnRenamed("customer_id", "customerId")
      .withColumnRenamed("merchant_id", "merchantId")
      .withColumnRenamed("card_id", "cardId")
      .as[Transaction]
  }

  def enrichWithDate(df: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    df.withColumn("date", to_date($"timestamp".cast(TimestampType), "yyyy-MM-dd"))
  }

  def buildCurrenciesGroupedByDate(df: DataFrame): DataFrame = {
    df
      .select("date", "currency")
      .groupBy("date")
      .agg(collect_set("currency").alias("currencies"))
  }

  def enrichWithFxRateFactor(df: DataFrame, convert: (LocalDate, Set[Currency]) => Map[Currency, MaybeConversionRate], sparkSession: SparkSession): Dataset[DatedConversionRate] = {
    import sparkSession.implicits._

    df.mapPartitions(partition => {

      partition.flatMap(record => {
        val date = record.getDate(0).toLocalDate
        val currencies = record.getSeq[Currency](1).toSet
        val rates = convert(date, currencies)
        rates.map { case (code, maybeRate) => DatedConversionRate(date, code, maybeRate) }
      })
    })
  }
}
