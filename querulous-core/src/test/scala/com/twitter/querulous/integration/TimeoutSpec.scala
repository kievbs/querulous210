package com.twitter.querulous.integration

import compat.Platform
import scala.concurrent.duration.{ Duration => D }
import scala.concurrent.duration._
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.database.ApachePoolingDatabaseFactory
import com.twitter.querulous.query.{TimingOutQueryFactory, SqlQueryTimeoutException}
import com.twitter.querulous.evaluator.{StandardQueryEvaluatorFactory}
import com.twitter.querulous.ConfiguredSpecification
import org.specs2.specification.BeforeExample

class TimeoutSpec extends ConfiguredSpecification with BeforeExample {
  import TestEvaluator._

  val timeout = D(1, SECONDS )
  val timingOutQueryFactory = new TimingOutQueryFactory(testQueryFactory, timeout, false)
  val apacheDatabaseFactory = new ApachePoolingDatabaseFactory(10, 10, D(1, SECONDS), D(10, MILLISECONDS), false, D(0, SECONDS) )
  val timingOutQueryEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, timingOutQueryFactory)

  def before = testEvaluatorFactory(config.withoutDatabase).execute("CREATE DATABASE IF NOT EXISTS db_test")

  "Timeouts" should {
    //skipIfCI {

      "honor timeouts" in {
        val queryEvaluator1 = testEvaluatorFactory(config)
        val dbLock = getDbLock(queryEvaluator1, "padlock")

        val thread = new Thread() {
          override def run() {
            try {
              Thread.sleep( 6000 )
            } catch { case _ => () }
            dbLock.countDown()
          }
        }
        thread.start()

        val queryEvaluator2 = timingOutQueryEvaluatorFactory(config)
        val start = Platform.currentTime
        queryEvaluator2.select("SELECT GET_LOCK('padlock', 60) AS rv") { row => row.getInt("rv") } must throwA[SqlQueryTimeoutException]
        val end = Platform.currentTime
        
        thread.interrupt()
        thread.join()
        (end - start) must beCloseTo(1000L,1000) //beCloseTo(timeout, 1000 )
      }
    //}
  }
}
