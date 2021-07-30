package homework.fraudio.conversion

import homework.fraudio.conversion.CurrencyLayerApi.parseBodyJson
import homework.fraudio.{ConversionRate, ConversionRateKey, Currency}
import io.circe.Decoder
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import java.time.LocalDate

/**
 * API to the REST service endpoint http://api.currencylayer.com/historical
 *
 * @param apiKey
 * @param url
 * @param source
 */
class CurrencyLayerApi(apiKey: String, url: String = "http://api.currencylayer.com/historical", source: String = "USD") {

  def getRates(date: LocalDate, currencies: Set[Currency]): Map[ConversionRateKey, ConversionRate] = {

    val queryParams = Map("access_key" -> apiKey, "date" -> date.toString, "source" -> source, "currencies" -> currencies.mkString(","))
    val pUri = uri"$url?$queryParams"
    val backend = HttpURLConnectionBackend() // TODO replace with an async client

    val maybeResult: Either[String, Map[String, ConversionRate]] = basicRequest
      .get(pUri)
      .send(backend)
      // TODO what if it does not returns 200?
      .body
      .map(parseBodyJson) match {
      case Left(error) => Left(error)
      case Right(Some(rates)) => Right(rates)
      case _ => Left(s"Error parsing the Historical Rates Response from $url")
    }

    maybeResult
      .map(_.map { case (name, rate) => (name.replaceFirst("^" + source, ""), rate) })
      .getOrElse(Map.empty)
      .map { case (currency, rate) => ((date, currency), rate) }
  }

}

object CurrencyLayerApi {

  def parseBodyJson(body: String): Option[Map[Currency, ConversionRate]] = {
    val quotesExtractor: Decoder[Map[String, ConversionRate]] = { c => c.downField("quotes").as[Map[String, ConversionRate]] }
    io.circe.parser.decode[Map[String, ConversionRate]](body)(quotesExtractor).toOption
  }

  def live(apiKey: String) = new CurrencyLayerApi(apiKey)
}