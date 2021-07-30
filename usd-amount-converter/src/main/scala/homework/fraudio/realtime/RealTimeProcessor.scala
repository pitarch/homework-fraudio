package homework.fraudio.realtime

import homework.fraudio.{ConvertedTransaction, RateConversionFunction, Transaction}

class RealTimeProcessor(convertFunc: RateConversionFunction) {

  def convert(transaction: Transaction): ConvertedTransaction = {
    val date = transaction.toDate
    val key = (date, transaction.currency)
    val maybeRate = convertFunc(date, Set(transaction.currency)).get(key)
    ConvertedTransaction(
      transaction_id = transaction.transaction_id,
      merchant_id  = transaction.merchant_id,
      card_id = transaction.card_id,
      timestamp = transaction.timestamp,
      amount = maybeRate.map(_ * transaction.amount)
    )
  }
}


object RealTimeProcessor {

  def apply(convertFunc: RateConversionFunction): RealTimeProcessor = new RealTimeProcessor(convertFunc)
}
