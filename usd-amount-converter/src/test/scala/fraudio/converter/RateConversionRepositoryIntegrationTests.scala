package fraudio.converter

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors}

import java.time.LocalDate

class RateConversionRepositoryIntegrationTests extends AnyFunSuite with Matchers with Inside with Inspectors {

  test("should get the historical rates for USD currency in some day from the rest api of CurrencyLayer.com") {

    val apiKey = "befda3866b99a5445b3e2c762f380fa1"
    val api = new RateConversionApi.RateConversionRepository(apiKey)
    val rates = api.getHistorical(LocalDate.now(), Set("EUR", "USD", "GBP", "AUD", "FOO"))
    rates("USD") shouldBe Some(1.0)
    rates("EUR").get should (be > 0.0)
    rates("GBP").get should (be > 0.0)
    rates("AUD").get should (be > 0.0)
    rates should not contain key ("FOO")
  }
}
