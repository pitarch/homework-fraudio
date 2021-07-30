package fraudio.converter

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors}


class CurrencyLayerHistorialRestResponseTests extends AnyFunSuite with Matchers with Inside with Inspectors {

  val response =
    """
      |    {
      |    "success": true,
      |    "terms": "https://currencylayer.com/terms",
      |    "privacy": "https://currencylayer.com/privacy",
      |    "historical": true,
      |    "date": "2005-02-01",
      |    "timestamp": 1107302399,
      |    "source": "USD",
      |    "quotes": {
      |        "USDAED": 3.67266,
      |        "USDALL": 96.848753,
      |        "USDAMD": 475.798297,
      |        "USDANG": 1.790403,
      |        "USDARS": 2.918969,
      |        "USDAUD": 1.293878
      |    }
      |}
      |""".stripMargin

  val expectedRates = Map(
    "USDAED" -> 3.67266,
    "USDALL" -> 96.848753,
    "USDAMD" -> 475.798297,
    "USDANG" -> 1.790403,
    "USDARS" -> 2.918969,
    "USDAUD" -> 1.293878)

  test("should parse historical rates response from currencylayer.com") {

    val result = RateConversionApi.parseHistoricalRatesResponse(response)
    inside(result) {
      case Some(rates) => rates should contain theSameElementsAs expectedRates
    }
  }
}
