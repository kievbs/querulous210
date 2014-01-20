package com.twitter.querulous.unit

import java.sql.ResultSet
import org.specs2.mock.Mockito //, ClassMocker}
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.test.FakeQuery
import com.twitter.querulous.query.{TimingOutQuery, SqlQueryTimeoutException}
import com.twitter.querulous.ConfiguredSpecification
import scala.concurrent.duration.{ Duration => D }
import scala.concurrent.duration._
import java.util.concurrent.{CountDownLatch, TimeUnit}

object TimingOutQuerySpec extends ConfiguredSpecification with Mockito { //with ClassMocker {
  "TimingOutQuery" should {
    //skipIfCI {
      val connection = TestEvaluator.testDatabaseFactory(
        config.hostnames.toList, config.username, config.password).open()
      val timeout = D(1, SECONDS )
      val resultSet = mock[ResultSet]

      "timeout" in {
        val latch = new CountDownLatch(1)
        val query = new FakeQuery(List(resultSet)) {
          override def cancel() = { latch.countDown() }

          override def select[A](f: ResultSet => A) = {
            latch.await(2000, TimeUnit.MILLISECONDS)
            super.select(f)
          }
        }
        val timingOutQuery = new TimingOutQuery(query, connection, timeout, true)

        timingOutQuery.select { r => 1 } must throwA[SqlQueryTimeoutException]
        latch.getCount mustEqual 0
      }

      "not timeout" in {
        val latch = new CountDownLatch(1)
        val query = new FakeQuery(List(resultSet)) {
          override def cancel() = { latch.countDown() }
        }
        val timingOutQuery = new TimingOutQuery(query, connection, timeout, true)

        timingOutQuery.select { r => 1 }
        latch.getCount mustEqual 1
      }
    }
  //}
}
