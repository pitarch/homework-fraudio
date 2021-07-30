package fraudio.converter

import org.apache.spark.sql.Encoders
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaTests extends AnyFunSuite with Matchers {



  test("create schema") {

    println(Encoders.product[Transaction].schema)
  }

}
