package homework.fraudio.conversion

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors}

import java.time.LocalDate

class CurrencyLayerApiIntegrationTests extends AnyFunSuite with Matchers with Inside with Inspectors {

  test("should get the historical rates for USD currency in some day from the rest api of CurrencyLayer.com") {

    val apiKey = "befda3866b99a5445b3e2c762f380fa1"
    val api = new CurrencyLayerApi(apiKey)
    val date = LocalDate.now()
    val rates = api.getRates(date, Set("EUR", "USD", "GBP", "AUD", "FOO"))
    rates((date, "USD")) shouldBe 1.0
    rates((date, "EUR")) should (be > 0.0)
    rates((date, "GBP")) should (be > 0.0)
    rates((date, "AUD")) should (be > 0.0)
    rates should not contain key((date, "FOO"))
  }
}
