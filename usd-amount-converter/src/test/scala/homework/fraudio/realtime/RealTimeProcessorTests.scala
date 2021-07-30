package homework.fraudio.realtime


import homework.fraudio.caching.{Jvm, addJvmCaching}
import homework.fraudio.realtime.RealTimeProcessorTests.countCallToConverter
import homework.fraudio.{RateConversionFunction, TestUtils, Transaction}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors, OptionValues}

import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicLong

class RealTimeProcessorTests extends AnyFunSuite with Matchers with OptionValues with Inside with Inspectors {

  test("should convert in real-time when there is rate conversion") {

    // Given a real-time rate conversor
    val processor = RealTimeProcessor(convertFunc = TestUtils.inverseConvert)

    // Given a transaction
    val date = OffsetDateTime.now()
    val transaction = Transaction(0, 0, "0", date.toEpochSecond, "EUR", 1.23)

    // When we convert the transaction amount
    val converted = processor.convert(transaction)

    // Then
    converted.amount.value shouldBe -1.23
  }


  test("should do not convert transaction in real-time when there is no rate conversion") {

    // Given a real-time rate conversor
    val converterFunc = TestUtils.emptyConvert
    val processor = RealTimeProcessor(convertFunc = converterFunc)

    // Given a transaction
    val date = OffsetDateTime.now()
    val transaction = Transaction(0, 0, "0", date.toEpochSecond, "EUR", 1.23)

    // Given there is no rate conversation for that date for the transaction currency
    converterFunc.apply(date.toLocalDate, Set(transaction.currency)) should have size 0

    // When we convert the transaction amount
    val converted = processor.convert(transaction)

    // Then
    converted.amount shouldBe None
  }


  test("should retrieve the conversation rate from the cache") {

    // Given a real-time rate conversor
    val counter = new AtomicLong()
    val fakeRateConverterApi = TestUtils.inverseConvert
    val fakeRateConverterApiWithCounter = countCallToConverter(counter, fakeRateConverterApi)
    val jvmCache = Jvm.createJvmCache
    val cachedConverterFunc = addJvmCaching(fakeRateConverterApiWithCounter, jvmCache)
    val processor = RealTimeProcessor(cachedConverterFunc)

    // Given a transaction
    val date = OffsetDateTime.now()
    val transaction = Transaction(0, 0, "0", date.toEpochSecond, "EUR", 1.23)

    // When we convert the transaction amount
    val converted = processor.convert(transaction)

    // Then the transaction amount is converted
    converted.amount.value shouldBe -1.23

    // Then the rest api is called
    counter.get() shouldBe 1

    // When we require a conversation for the same date and currency
    val converted2ndTime = processor.convert(transaction)

    // Then transaction amount is converted again
    converted2ndTime.amount.value shouldBe -1.23

    // Then the conversation is retrieved from the cache
    counter.get() shouldBe 1
  }
}


object RealTimeProcessorTests {

  def countCallToConverter(counter: AtomicLong, converterFunc: RateConversionFunction): RateConversionFunction = (date, currencies) => {
    counter.incrementAndGet
    converterFunc(date, currencies)
  }
}