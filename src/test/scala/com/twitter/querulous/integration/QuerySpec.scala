package com.twitter.querulous.integration

import com.twitter.querulous.ConfiguredSpecification
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.query._
import org.specs2.mutable._

class QuerySpec extends ConfiguredSpecification {
  sequential
//  Configgy.configure("config/" + System.getProperty("stage", "test") + ".conf")

//  val config = Configgy.config.configMap("db")
//  val username = config("username")
//  val password = config("password")
//  val queryEvaluator = testEvaluatorFactory("localhost", "db_test", username, password)
  import TestEvaluator._
  val queryEvaluator = testEvaluatorFactory(config)

  implicit lazy val b = new Before {
    def before = {
      queryEvaluator.execute("CREATE TABLE IF NOT EXISTS foo(bar INT, baz INT)")
      queryEvaluator.execute("TRUNCATE foo")
      queryEvaluator.execute("INSERT INTO foo VALUES (1,1), (3,3)")
    }
  } 

  "Query" should {
    //skipIfCI {

      "with too many arguments" >> {
        queryEvaluator.select("SELECT 1 FROM DUAL WHERE 1 IN (?)", 1, 2, 3) { r => 1 } must throwA[TooManyQueryParametersException]
      }

     

      "in batch mode" in {
        queryEvaluator.executeBatch("UPDATE foo SET bar = ? WHERE bar = ?") { withParams =>
          withParams("2", "1")
          withParams("3", "3")
        } mustEqual 2
      }

     "with too few arguments" in {
        queryEvaluator.select("SELECT 1 FROM DUAL WHERE 1 = ? OR 1 = ?", 1) { r => 1 } must throwA[TooFewQueryParametersException]
      } 

      "add annotations" >> {
        val connection = testDatabaseFactory(config.hostnames.toList, config.database, config.username,
          config.password, config.urlOptions, config.driverName).open()

        try {
          val query = testQueryFactory(connection, QueryClass.Select, "SELECT 1")
          query.addAnnotation("key", "value")
          query.addAnnotation("key2", "value2")
          query.select(rv => rv.getInt(1) mustEqual 1)
        } finally {
          connection.close()
        }
      }

      "with just the right number of arguments" >> {
        queryEvaluator.select("SELECT 1 FROM DUAL WHERE 1 IN (?)", List(1, 2, 3))(_.getInt(1)).toList mustEqual List(1)
      }

      "be backwards compatible" >> {
        val noOpts = testEvaluatorFactory(config.hostnames.toList, null, config.username, config.password)
        noOpts.select("SELECT 1 FROM DUAL WHERE 1 IN (?)", List(1, 2, 3))(_.getInt(1)).toList mustEqual List(1)

        val noDBNameOrOpts = testEvaluatorFactory(config.hostnames.toList, config.username, config.password)
        noDBNameOrOpts.select("SELECT 1 FROM DUAL WHERE 1 IN (?)", List(1, 2, 3))(_.getInt(1)).toList mustEqual List(1)
      } 
    //}
  }
}
