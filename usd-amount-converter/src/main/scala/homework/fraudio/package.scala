package homework

import java.time.{Instant, LocalDate, ZoneOffset}

package object fraudio {

  type Currency = String
  type Amount = Double
  type ConversionRate = Double
  type ConversionRateKey = (LocalDate, Currency)
  type RateConversionFunction = (LocalDate, Set[Currency]) => Map[ConversionRateKey, ConversionRate]

  case class Transaction(transaction_id: Long,
                         merchant_id: Int,
                         card_id: String,
                         timestamp: Long,
                         currency: Currency,
                         amount: Amount
                        ) {
    def toDate: LocalDate = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC).toLocalDate
  }


  case class DatedConversionRate(date: LocalDate, currency: Currency, rate: ConversionRate)

  case class ConvertedTransaction(
                                   transaction_id: Long,
                                   merchant_id: Int,
                                   card_id: String,
                                   timestamp: Long,
                                   amount: Option[Amount]
                                 )

  object Transaction {

    def apply(transaction_id: Long, merchant_id: Int, card_id: String, timestamp: Long, currency: Currency, amount: Amount): Transaction =
      new Transaction(transaction_id, merchant_id, card_id, timestamp, currency, amount)
  }

}

