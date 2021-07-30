package fraudio.converter

import fraudio.converter.BatchProcessing.{buildCurrenciesGroupedByDate, enrichWithDate}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.types.{ArrayType, DateType, StringType}

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}

class BatchProcessingTests extends IntegrationTestBase {

  test("cleanse should produce a Dataset[Transaction]") {
    val data = Seq((0, 0, "0", 1609455985L, "USD", 77.44)).map(ttt => Row(ttt.productIterator.toSeq: _*))

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, inputTransactionSchema)
    val ds = BatchProcessing.cleanse(df)
    val expectedSchema = Encoders.product[Transaction].schema


    forAll(expectedSchema) { expectedField =>
      forAtLeast(1, ds.schema) { actualField =>
        actualField.name shouldBe expectedField.name
        actualField.dataType shouldBe expectedField.dataType
      }
    }
  }

  test("cleanse should remove records with wrong ISO 4217 Alpha codes") {

    val template = Transaction(0, 0, "0", 1609455985L, "USD", 77.44)
    val currencies = Seq[String](null, "dollar", "978")
    val transactions = currencies.map(curr => template.copy(currency = curr))
    val df = spark.createDataFrame(transactions)
    BatchProcessing.cleanse(df).collect() should have length 0
  }

  test("cleanse should admit records complying ISO 4217 Alpha codes") {

    val template = Transaction(0, 0, "0", 1609455985L, "USD", 77.44)
    val currencies = Seq[String]("USD", "XXX", "ABC")
    val transactions = currencies.map(curr => template.copy(currency = curr))
    val df = spark.createDataFrame(transactions)
    BatchProcessing.cleanse(df).select("currency").collect().map(_.getString(0)) should contain theSameElementsAs currencies
  }


  test("enrich with Date") {
    val transaction = Transaction(0, 0, "0", 1609455985L, "USD", 77.44)
    val initalDf = spark.createDataFrame(Seq(transaction))
    val df = BatchProcessing.enrichWithDate(initalDf, spark)
    val rows = df.select("date").collect()

    df.schema.exists(struct => struct.name == "date" && struct.dataType == DateType) shouldBe true
    rows should have length 1
    rows.head.getDate(0).toString shouldBe "2021-01-01"
  }


  test("buildCurrenciesGroupedByDate should group currencies by date") {

    val instant = Instant.parse("2021-01-01T00:00:00Z")
    val instants = Seq(instant, instant.plus(1, ChronoUnit.DAYS))
    val currencies = Seq("USD", "AUD")
    val template = Transaction(0, 0, "0", 0, "_", 77.44)
    val transactions = for {
      instant <- instants
      currency <- currencies
    } yield template.copy(timestamp = instant.getEpochSecond, currency = currency)

    val df = spark.createDataFrame(transactions)
    val groupedByDateDf = buildCurrenciesGroupedByDate(enrichWithDate(df, spark))

    val schema = groupedByDateDf.schema
    schema.exists(field => field.name == "date" && field.dataType == DateType) shouldBe true
    schema.exists(field => field.name == "currencies" && field.dataType == ArrayType(StringType, containsNull = false)) shouldBe true

    val rows = groupedByDateDf
      .sort("date")
      .collect()

    rows should have length 2
    rows(0).getDate(0).toString shouldBe "2021-01-01"
    rows(0).getList[String](1) should contain theSameElementsAs currencies
    rows(1).getDate(0).toString shouldBe "2021-01-02"
    rows(1).getList[String](1) should contain theSameElementsAs currencies
  }

  test("buildCurrenciesGroupedByDate should remove duplicates for the same date") {

    val instant = Instant.parse("2021-01-01T00:00:00Z")
    val transaction = Transaction(0, 0, "0", instant.getEpochSecond, "USD", 77.44)
    val transactions = Seq(transaction, transaction)

    val initalDf = spark.createDataFrame(transactions)
    val df = buildCurrenciesGroupedByDate(enrichWithDate(initalDf, spark))
    val rows = df.sort("date").collect()

    rows should have length 1
    rows(0).getDate(0).toString shouldBe "2021-01-01"
    rows(0).getList[String](1) should contain theSameElementsAs Set("USD")
  }


  test("enrichWithFxRateFactor") {
    import spark.implicits._

    val convert: (LocalDate, Set[Currency]) => Map[Currency, MaybeConversionRate] =
      (_, currencyCodes) => currencyCodes.map { code => (code, Some(-1.0)) }.toMap

    val date = LocalDate.now()
    val dates = Seq(date, date.minusDays(1))
    val currencies = Set("USD", "EUR", "GBP")

    val df = dates.map((_, currencies)).toDF()
    val rateByDateDf = BatchProcessing.enrichWithFxRateFactor(df, convert, spark).cache()

    val schema = rateByDateDf.schema
    schema.exists(field => field.name == "date")
    schema.exists(field => field.name == "currency")
    schema.exists(field => field.name == "rate")

    rateByDateDf.show()

    val rates = rateByDateDf.collectAsList()

    /**
     * expected
     * +----------+--------+----+
     * |      date|currency|rate|
     * +----------+--------+----+
     * |2021-07-29|     USD|-1.0|
     * |2021-07-29|     EUR|-1.0|
     * |2021-07-29|     GBP|-1.0|
     * |2021-07-28|     USD|-1.0|
     * |2021-07-28|     EUR|-1.0|
     * |2021-07-28|     GBP|-1.0|
     * +----------+--------+----+
     */
    forAll(dates) { date =>
      forAll(currencies) { currency =>
        forAtLeast(1, rates) { rate =>
          rate.date shouldBe date
          rate.currency shouldBe currency
          rate.rate shouldBe Some(-1.0)
        }
      }
    }
  }
}
