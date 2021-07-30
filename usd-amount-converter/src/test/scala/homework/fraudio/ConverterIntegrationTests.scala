package homework.fraudio

import homework.fraudio.TestUtils.{countCallsToConverter, inverseConvert}
import homework.fraudio.caching.{Jvm, addJvmAndRedisCaching}

import java.time.OffsetDateTime

class ConverterIntegrationTests extends IntegrationTestBase {

  import spark.implicits._

  test("should convert real-time transaction") {

    // Given a fake REST API to currencylayer.com
    val counter = spark.sparkContext.longAccumulator("restApiCalls")
    val fakeConverterRestApi = countCallsToConverter(counter, inverseConvert)

    // Given a converter
    val converter: Converter = Converter(fakeConverterRestApi)

    // Given a transaction
    val date = OffsetDateTime.now()
    val transaction = Transaction(0, 0, "0", date.toEpochSecond, "EUR", 1.23)

    // When we convert the transaction
    val converted = converter.convertInRealTime(transaction)

    // Then the transaction amount should be converted
    converted.amount.value shouldBe -1.23
  }

  test("should convert real-time transaction using the cache") {

    // Given a fake for the REST API to currencylayer.com
    val counter = spark.sparkContext.longAccumulator("restApiCalls")
    val fakeConverterRestApi = countCallsToConverter(counter, inverseConvert)

    // Given a caching mechanism to the underlying rate conversion provider
    val jvmCache = Jvm.createJvmCache
    val convertFunc = addJvmAndRedisCaching(fakeConverterRestApi, jvmCache)

    // Given a converter
    val converter: Converter = Converter(convertFunc)

    // Given a transaction
    val date = OffsetDateTime.now()
    val transaction = Transaction(0, 0, "0", date.toEpochSecond, "EUR", 1.23)

    // When we convert the transaction
    val converted = converter.convertInRealTime(transaction)

    // Then the transaction amount should be converted
    converted.amount.value shouldBe -1.23

    // Then the rest api is call
    counter.count shouldBe 1

    // Then the conversion is cached
    jvmCache should contain key(date.toLocalDate, "EUR")

    // When we convert another transaction for the same date and currency
    converter.convertInRealTime(transaction).amount.value shouldBe -1.23

    // Then the cache is used instead of the calling the rest api
    counter.count shouldBe 1
  }


  test("should convert transaction in batch") {

    // Given a fake for the REST API to currencylayer.com
    val counter = spark.sparkContext.longAccumulator("restApiCalls")
    val fakeConverterRestApi = countCallsToConverter(counter, inverseConvert)

    // Given a converter
    val converter: Converter = Converter(fakeConverterRestApi)

    // Given a transaction
    val date = OffsetDateTime.now()
    val transaction = Transaction(0, 0, "0", date.toEpochSecond, "EUR", 1.23)

    // When we convert the transaction
    val converted = converter.convertInBatch(Seq(transaction).toDS()).collect()

    // Then the transaction amount should be converted
    converted should have length 1
    converted(0).amount.value shouldBe -1.23
  }
}
