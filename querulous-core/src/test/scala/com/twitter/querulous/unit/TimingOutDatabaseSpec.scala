package com.twitter.querulous.unit

import com.twitter.querulous._
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.sql.Connection
import com.twitter.querulous.database.{SqlDatabaseTimeoutException, TimingOutDatabase}
import com.twitter.querulous.test.FakeDatabase
import compat.Platform
import scala.concurrent.duration._
import org.specs2._ //Specification
import org.specs2.mock._ //, ClassMocker}

/*
class TimingOutDatabaseSpec extends Specification with Mockito { // with ClassMocker {
  // @TODO figure out why this won't compile
  //"TimingOutDatabaseSpec" should {
    /*"totally timeout" in {
      val latch      = new CountDownLatch(1)
      val timeout    = Duration(1, SECONDS )
      var shouldWait = false
      val connection = mock[Connection]
      val future     = new FutureTimeout(1, 1)
      val database   = new FakeDatabase {
        def open() = {
          if (shouldWait) latch.await(100 * 1000, TimeUnit.MILLISECONDS)
          connection
        }
        def close(connection: Connection) = ()
      }

there was one(connection).close()
    //expect {
//      one(connection).close()
    //}

    val timingOutDatabase = new TimingOutDatabase(database, future, timeout)
    shouldWait = true


      //try {
        val epsilon = Duration(150, MILLISECONDS )
        var start = Platform.currentTime
        timingOutDatabase.open() must throwA[SqlDatabaseTimeoutException]
        var end = Platform.currentTime
        
      //} finally {
        latch.countDown()
      //}
(end.toMillis - start.toMillis) must beCloseTo(timeout.toMillis, epsilon.toMillis)
          
    }*/
  //}
}*/
