package homework.fraudio.batch

import homework.fraudio.TestUtils.countCallsToConverter
import homework.fraudio.batch.BatchProcessor.convert
import homework.fraudio.{IntegrationTestBase, RateConversionFunction, TestUtils, Transaction}
import org.apache.spark.sql.{Dataset, Row}

import java.sql.Date
import java.time.{Instant, LocalDate, OffsetDateTime, ZoneOffset}

class BatchProcessorTests extends IntegrationTestBase {

  import spark.implicits._

  test("the historical rate conversation table should contain a rate for each pair (date, currency) that ") {

    // Given a date
    val date = LocalDate.now()

    // Given a set of currencies
    val currencies = Seq("EUR", "GBP")

    // Given the aggregation row of the previous currencies with the previous date
    val row = Row(Date.valueOf(date), currencies)

    // Given a partition of that row
    val partition = Seq(row).toIterator

    // Given there exist conversation of all the currencies for that date
    val convertFunc = TestUtils.doubleConvert

    // When
    val datedRates = BatchProcessor.processPartitionOfAggCurrenciesByDate(partition, convertFunc).toList

    // Then for the currencies there exists a conversation rate in that date
    datedRates should have length 2
    forAll(currencies) { currency =>
      forAtLeast(1, datedRates) { datedRated =>
        datedRated.currency shouldBe currency
        datedRated.date shouldBe date
        datedRated.rate shouldBe 2.0
      }
    }
  }

  test("should convert the transaction amount when there exists a conversation rate") {
    // Given a transaction
    val date = OffsetDateTime.now()
    val transaction = Transaction(0, 0, "0", date.toEpochSecond, "EUR", 1.23)

    // Given a dataset of that transactionDs
    val transactions: Dataset[Transaction] = Seq(transaction).toDS()

    // Given that there exist a conversation rate for the date of the transaction
    val converter = TestUtils.doubleConvert
    converter(date.toLocalDate, Set("EUR")).head shouldBe((date.toLocalDate, "EUR"), 2.0)

    // When we convert that transaction
    val result = convert(transactions, converter).collect()

    // Then
    result should have length 1
    result(0).amount.value shouldBe transaction.amount * 2.0
    result(0).transaction_id shouldBe transaction.transaction_id
    result(0).merchant_id shouldBe transaction.merchant_id
    result(0).card_id shouldBe transaction.card_id
    result(0).timestamp shouldBe transaction.timestamp
  }


  test("different rates will produce different amounts when converted") {

    // Given two different dates
    val date1 = Instant.now().atOffset(ZoneOffset.UTC)
    val date2 = date1.minusDays(1)

    // Given two transactionDs on each of the previous dates
    val t1 = Transaction(0, 0, "0", date1.toEpochSecond, "EUR", 1.20)
    val t2 = t1.copy(transaction_id = 1, timestamp = date2.toEpochSecond)

    // Given two different conversation rates those dates
    val histRates = Map((date1.toLocalDate, "EUR") -> 2.0, (date2.toLocalDate, "EUR") -> 0.5)
    val converterFunc: RateConversionFunction = (_, _) => histRates

    // Given a dataset of both transactionDs
    val transactionDs: Dataset[Transaction] = Seq(t1, t2).toDS()

    // When we convert those transactionDs...
    val convertedTransactions = convert(transactionDs, converterFunc).collectAsList()

    // Then
    convertedTransactions should have length 2
    forAtLeast(1, convertedTransactions) { transaction =>
      transaction.transaction_id shouldBe t1.transaction_id
      transaction.timestamp shouldBe t1.timestamp
      transaction.amount shouldBe Some(t1.amount * 2.0)
    }

    forAtLeast(1, convertedTransactions) { transaction =>
      transaction.transaction_id shouldBe t2.transaction_id
      transaction.timestamp shouldBe t2.timestamp
      transaction.amount shouldBe Some(t2.amount * 0.5)
    }
  }


  test("transactionDs with no conversion rate shows 'None' in their amount") {

    // Given a transaction
    val date = OffsetDateTime.now()
    val transaction = Transaction(0, 0, "0", date.toEpochSecond, "EUR", 1.23)

    // Given a dataset of that transactionDs
    val transactions: Dataset[Transaction] = Seq(transaction).toDS()

    // Given that no conversation rate exist for date of the transaction
    val converter = TestUtils.emptyConvert
    converter(date.toLocalDate, Set("EUR")).isEmpty shouldBe true

    // When we convert that transaction
    val result = convert(transactions, converter).collect()

    // Then
    result should have length 1
    result(0).amount shouldBe None
    result(0).transaction_id shouldBe transaction.transaction_id
    result(0).merchant_id shouldBe transaction.merchant_id
    result(0).card_id shouldBe transaction.card_id
    result(0).timestamp shouldBe transaction.timestamp
  }


  test("should call currencyLayer once per date") {

    val transactionDs = readTestCsv("src/test/resources/transactions.csv")
      .toDF()
      .as[Transaction].cache()

    val counter = spark.sparkContext.longAccumulator("restApiCounter")
    val converterFunc = countCallsToConverter(counter, TestUtils.inverseConvert)

    val maxExpectedCalls = transactionDs.map(_.toDate.toString).distinct().count()
    BatchProcessor.convert(transactionDs, converterFunc).foreach(_ => ())

    counter.count should (be <= maxExpectedCalls)
  }
}
