package fraudio.converter

import io.circe.Decoder
import sttp.client3._

import java.time.LocalDate

object RateConversionApi {

  /**
   * extract the rate conversions returned from the CurrencyLayer.com endpoint /historical
   * @param body json-formatted response body
   * @return a map with the conversion date for each currency
   */
  def parseHistoricalRatesResponse(body: String): Option[Map[Currency, ConversionRate]] = {
    val quotesExtractor: Decoder[Map[String, ConversionRate]] = { c => c.downField("quotes").as[Map[String, ConversionRate]] }
    io.circe.parser.decode[Map[String, ConversionRate]](body)(quotesExtractor).toOption
  }

  class RateConversionRepository(apiKey: String) {

    /**
     * Retrieve the the USD rate conversion to some currency in some date
     * @param date
     * @param currency
     * @return
     */
    def getRate(date: LocalDate, currency: Currency): MaybeConversionRate = getHistorical(date, Set(currency))(currency)

    /**
     * Retrieve the FX conversion rate of a USD currency into different currencies in some date
     *
     * It accesses the REST API of http://api.currencylayer.com/historical
     *
     * @param date  date
     * @param codes currencies
     * @return a map of the target currency with the conversation to the the source currency
     */
    def getHistorical(date: LocalDate, codes: Set[Currency]): Map[Currency, MaybeConversionRate] = {
      val url = "http://api.currencylayer.com/historical" // TODO provide the URL from the component config
      val queryParams = Map("access_key" -> apiKey, "date" -> date.toString, "source" -> "USD", "currencies" -> codes.mkString(","))
      val pUri = uri"$url?$queryParams"
      val backend = HttpURLConnectionBackend() // TODO replace with an async client

      val maybeResult: Either[String, Map[String, ConversionRate]] = basicRequest
        .get(pUri)
        .send(backend)
        // TODO what if it does not returns 200?
        .body
        .map(parseHistoricalRatesResponse) match {
        case Left(error) => Left(error)
        case Right(Some(rates)) => Right(rates)
        case _ => Left(s"Error parsing the Historical Rates Response from $url")
      }

      maybeResult
        .map(_.map { case (name, rate) => (name.replaceFirst("^USD", ""), Some(rate)) })
        .getOrElse(Map.empty)
        .withDefaultValue(None)
    }
  }
}
