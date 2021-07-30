package homework.fraudio

import homework.fraudio.IntegrationTestBase.inputTransactionSchema
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inside, Inspectors, OptionValues}


abstract class IntegrationTestBase extends AnyFunSuite with Matchers with OptionValues with Inside with Inspectors
  with BeforeAndAfterAll {

  /** Provides Spark context */
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("FraudioHomework")
    .setMaster("local[*]")
    .set("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
  val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

//  override def afterAll(): Unit = {
//    spark.stop()
//  }


  protected def readTestCsv(path: String): DataFrame = spark.read
    .option("header", value = true)
    .option("mode", "DROPMALFORMED") // ignore malformed records
    .schema(inputTransactionSchema)
    .csv(path)
}


object IntegrationTestBase {

  val inputTransactionSchema: StructType = StructType(
    List(
      StructField("transaction_id", LongType),
      StructField("customer_id", IntegerType),
      StructField("merchant_id", IntegerType),
      StructField("card_id", StringType),
      StructField("timestamp", LongType, nullable = false),
      StructField("currency", StringType, nullable = false),
      StructField("amount", DoubleType, nullable = false),
    )
  )

}