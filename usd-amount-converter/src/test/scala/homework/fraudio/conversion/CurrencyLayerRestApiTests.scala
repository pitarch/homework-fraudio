package homework.fraudio.conversion

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors}


class CurrencyLayerRestApiTests extends AnyFunSuite with Matchers with Inside with Inspectors {

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

  test("should parse the response from currencylayer.con containing the historical rates") {

    val result = CurrencyLayerApi.parseBodyJson(response)
    inside(result) {
      case Some(rates) => rates should contain theSameElementsAs expectedRates
    }
  }
}
