package com.twitter.querulous.unit

import java.sql.Connection
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito //{ClassMocker, Mockito}
import com.twitter.querulous.database.{SqlDatabaseTimeoutException, StatsCollectingDatabase}
import com.twitter.querulous.test.{FakeStatsCollector, FakeDBConnectionWrapper}
import compat.Platform
import scala.concurrent.duration.{Duration => D}
import scala.concurrent.duration._

//@TODO how to handle this timing shiz..?
/*
class StatsCollectingDatabaseSpec extends Specification with Mockito { //with ClassMocker {
  "StatsCollectingDatabase" ignore {
    val latency = D(1, SECONDS )
    val connection :Connection = mock[Connection]
    val stats = new FakeStatsCollector
    def pool(callback: String => Unit) = new StatsCollectingDatabase(
      new FakeDBConnectionWrapper(connection, callback),
      "test",
      stats
    )
    
    "collect stats" in {
      "when closing" >> {
        //? Time.withCurrentTimeFrozen { time =>
          pool(s => /*time.advance(latency)*/ () ).close(connection)
          stats.times("db-close-timing") mustEqual latency.toMillis
        //}
      }

      "when opening" >> {
        // ?Time.withCurrentTimeFrozen { time =>
          pool(s => /*time.advance(latency)*/ ()).open()
          stats.times("db-open-timing") mustEqual latency.toMillis
        //}
      }
    }

    "collect timeout stats" in {
      val e = new SqlDatabaseTimeoutException("foo", D( 0, SECONDS ) )
      "when closing" >> {
        pool(s => throw e).close(connection) must throwA[SqlDatabaseTimeoutException]
        stats.counts("db-close-timeout-count") mustEqual 1
        stats.counts("db-test-close-timeout-count") mustEqual 1
      }

      "when opening" >> {
        //? Time.withCurrentTimeFrozen { time =>
          pool(s => throw e).open() must throwA[SqlDatabaseTimeoutException]
          stats.counts("db-open-timeout-count") mustEqual 1
          stats.counts("db-test-open-timeout-count") mustEqual 1
       // }
      }
    } 
  }
}
*/