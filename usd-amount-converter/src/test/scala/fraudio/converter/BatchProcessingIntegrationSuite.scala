package fraudio.converter

import fraudio.converter.BatchProcessingIntegrationSuite.usdConvert

import java.time.LocalDate

class BatchProcessingIntegrationSuite extends IntegrationTestBase {

  test("enrichWithFxRateFactor should retrieve rates from an external provider") {
    import spark.implicits._

    val date = LocalDate.now()
    val dates = Seq(date, date.minusDays(1))
    val currencies = Set("USD", "EUR", "GBP")

    val df = dates.map( (_, currencies) ).toDF()
    val histRatesByDate = BatchProcessing.enrichWithFxRateFactor(df, usdConvert, spark).cache()

    histRatesByDate.show(100)
    // validate resulting schema
    val schema = histRatesByDate.schema
    schema.exists(field => field.name == "date") shouldBe true
    schema.exists(field => field.name == "currency") shouldBe true
    schema.exists(field => field.name == "rate") shouldBe true

    val rates = histRatesByDate.collectAsList()


    forAll(dates) { date =>
      forAll(currencies) { currency =>
        forAtLeast(1, rates) { rate =>
          rate.date shouldBe date
          rate.currency shouldBe currency
          //rate.rate should (be > 0.0d)
        }
      }
    }
  }
}


object BatchProcessingIntegrationSuite {

  val repository: RateConversionApi.RateConversionRepository = new RateConversionApi.RateConversionRepository("befda3866b99a5445b3e2c762f380fa1")
  val usdConvert: (LocalDate, Set[Currency]) => Map[Currency, MaybeConversionRate] = repository.getHistorical
}