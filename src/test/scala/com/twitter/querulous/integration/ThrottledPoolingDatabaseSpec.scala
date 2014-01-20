package com.twitter.querulous.integration

import com.twitter.querulous.ConfiguredSpecification
import com.twitter.querulous.database.{SqlDatabaseTimeoutException, ThrottledPoolingDatabaseFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import scala.concurrent.duration.{Duration => D}
import scala.concurrent.duration._

object ThrottledPoolingDatabaseSpec {
  val dur = D(1,SECONDS)
  val testDatabaseFactory = new ThrottledPoolingDatabaseFactory(1, dur, dur, dur, Map.empty)
  val testQueryFactory = new SqlQueryFactory
  val testEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, testQueryFactory)
}

class ThrottledPoolingDatabaseSpec extends ConfiguredSpecification {
  import ThrottledPoolingDatabaseSpec._
  sequential

  "ThrottledJdbcPoolSpec" should {
    //skipIfCI {
      val queryEvaluator = testEvaluatorFactory(config)

      "execute some queries" >> {
        queryEvaluator.select("SELECT 1 FROM DUAL") { _.getInt(1) } mustEqual List(1)
        queryEvaluator.select("SELECT 2 FROM DUAL") { _.getInt(1) } mustEqual List(2)
      }

      "timeout when attempting to get a second connection" >> {
        queryEvaluator.select("SELECT 1 FROM DUAL") { r =>
          queryEvaluator.select("SELECT 2 FROM DUAL") { r2 => } must throwA[SqlDatabaseTimeoutException]
        }
      } 

      "ejects idle connections" >> {
        queryEvaluator.execute("set session wait_timeout = 1")
        Thread.sleep(2000)
        queryEvaluator.select("SELECT 1 FROM DUAL") { _.getInt(1) } mustEqual List(1)
      }
    //}
  }
}