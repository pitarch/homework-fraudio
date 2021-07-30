package fraudio.converter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inside, Inspectors, OptionValues}


class IntegrationTestBase extends AnyFunSuite with Matchers with OptionValues with Inside with Inspectors
  with BeforeAndAfterAll {

  /** Provides Spark context */
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("Foo")
    .setMaster("local[*]")
    .set("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
  //  Logger.getLogger("org.apache.spark").setLevel(Level.toLevel("ERROR"))
  val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
  //
  //  val testInputPath = "./src/test/resources"
  //  val testOutputPath = "./target/test-output"
  //
  //  /**
  //   * Before each test suite, make sure the test-output folder exists and is empty
  //   */
  //  override def beforeAll(): Unit = {
  //    val outputDir = new File(testOutputPath)
  //    outputDir.mkdirs()
  //    FileUtils.cleanDirectory(outputDir)
  //  }
  //
  //  /**
  //   * Clean up the test-output folder after the test suite has run
  //   */
  //  override def afterAll(): Unit = {
  //    FileUtils.deleteDirectory(new File(testOutputPath))
  //  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
