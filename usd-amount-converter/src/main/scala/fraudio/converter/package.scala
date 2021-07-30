package fraudio

import org.apache.spark.sql.types._

import java.time.{Instant, LocalDate, ZoneOffset}

package object converter {

  type Currency = String
  type Amount = Double
  type ConversionRate = Double
  type MaybeConversionRate = Option[ConversionRate]
  val inputTransactionSchema: StructType = StructType(
    List(
      StructField("transaction_id", LongType),
      StructField("customer_id", IntegerType),
      StructField("merchant_id", IntegerType),
      StructField("card_id", StringType),
      StructField("timestamp", LongType, nullable = false),
      StructField("currency", StringType, nullable = false),
      StructField("amount", DoubleType, nullable = false),
    )
  )


  case class DatedConversionRate(date: LocalDate, currency: Currency, rate: MaybeConversionRate)

  case class Transaction(
                          transactionId: Long,
                          merchantId: Int,
                          cardId: String,
                          timestamp: Long,
                          currency: Currency,
                          amount: Amount
                        ) {
    def toDate: LocalDate = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC).toLocalDate
  }
}
