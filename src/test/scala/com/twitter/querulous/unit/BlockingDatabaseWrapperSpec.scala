package com.twitter.querulous.unit

import org.specs2.mutable._
import java.sql.Connection
import java.util.concurrent.atomic._
import java.util.concurrent.RejectedExecutionException
import com.twitter.querulous.database._
import com.twitter.querulous.query._
import com.twitter.querulous.async._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.duration.{Duration => D}
import java.util.concurrent.Executors


class BlockingDatabaseWrapperSpec extends Specification {
  sequential 
  "BlockingDatabaseWrapper" should {
    val to = D(1,SECONDS)
    val database = new DatabaseProxy {
      var database: Database = null // don't care
      val totalOpens         = new AtomicInteger(0)
      val openConns          = new AtomicInteger(0)

      // override the methods BlockingDatabaseWrapper uses.
      override def openTimeout = D(500,MILLISECONDS)
      override def hosts = List("localhost")
      override def name = "test"

      def open() = {
        totalOpens.incrementAndGet
        openConns.incrementAndGet
        null.asInstanceOf[Connection]
      }

      def close(c: Connection) = { openConns.decrementAndGet }

      def reset() = { totalOpens.set(0); openConns.set(0) }
    }

    

    trait DbWrapper extends BeforeAfter {
      lazy val threads = Executors.newCachedThreadPool

      lazy val wrapper = new BlockingDatabaseWrapper(database)(ExecutionContext.fromExecutor(threads))

      def before = database.reset

      def after = threads.shutdownNow
    }

    "withConnection should follow connection lifecycle" in new DbWrapper { 
      Await.result( wrapper.withConnection( _ => "DONE" ), to ) //apply()

      database.totalOpens.get mustEqual 1
      database.openConns.get  mustEqual 1
    }

    "withConnection should return connection on exception" in new DbWrapper { 
      Await.result( wrapper withConnection { _ => throw new Exception } recover { case _ => "Done with Exception" } , to )//apply()

      database.totalOpens.get mustEqual 1
      database.openConns.get  mustEqual 0
    }

    "withConnection should not be interrupted if already executing" in new DbWrapper { 
      val result = Await.result( wrapper withConnection { _ =>
        Thread.sleep(1000)
        "Done"
      }, to ) //apply()

      result mustEqual "Done"
      database.totalOpens.get mustEqual 1
      database.openConns.get  mustEqual 1
    }
    /*
    "withConnection should follow lifecycle regardless of cancellation" in new DbWrapper {
      skipped("")
      val hitBlock = new AtomicInteger(0)
      val futures = for (i <- 1 to 100000) yield {
        val f = wrapper.withConnection { _ =>
          hitBlock.incrementAndGet
          "Done"
        } recover {
          case e => "Cancelled"
        }

        //f.cancel()
        f
      }

      val results = Await.result( Future.sequence(futures), D(5,SECONDS) )//.apply()
      val completed = results partition { _ == "Done" } _1


      // println debugging
      println("Opened:    "+ database.totalOpens.get)
      println("Ran block: "+ hitBlock.get)
      println("Cancelled: "+ (100000 - completed.size))
      println("Completed: "+ completed.size)
      println("Cached:    "+ database.openConns.get)

      true mustEqual true
    }         */

    "withConnection should respect open timeout and max waiters" in new DbWrapper { 
      val futures = for (i <- 1 to 10000) yield {
        wrapper.withConnection { _ =>
          //Thread.sleep(database.openTimeout.toMillis * 2)
          "Done"
        } recover {
          case e: RejectedExecutionException => "Rejected"
          case e: TimeoutException           => "Timeout"
          case _: Throwable                  => "Failed"
        }
      }

      val results = Await.result( Future.sequence(futures),D(5,SECONDS)  )//.await() //pply()
      val completed = results count { _ == "Done" }
      val rejected = results count { _ == "Rejected" }
      val timedout = results count { _ == "Timeout" }

      rejected mustEqual (results.size - completed - timedout)

    } 
  }
}
