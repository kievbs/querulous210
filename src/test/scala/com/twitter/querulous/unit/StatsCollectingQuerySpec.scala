package com.twitter.querulous.unit

import java.sql.ResultSet
import org.specs2.mutable._
import org.specs2.mock.Mockito
import com.twitter.querulous.query.{QueryClass, SqlQueryTimeoutException, StatsCollectingQuery}
import com.twitter.querulous.test.{FakeQuery, FakeStatsCollector}
import compat.Platform
import scala.concurrent.duration.{ Duration => D }
import scala.concurrent.duration._

class StatsCollectingQuerySpec extends Specification with Mockito {
  "StatsCollectingQuery" should {
    "collect stats" in {
      // @TODO how to fix this time shiz??
     //? Time.withCurrentTimeFrozen { time =>
        val latency = D(1, SECONDS )
        val stats = new FakeStatsCollector
        val testQuery = new FakeQuery(List(mock[ResultSet])) {
          override def select[A](f: ResultSet => A) = {
            //? time.advance(latency)
            super.select(f)
          }
        }
        val statsCollectingQuery = new StatsCollectingQuery(testQuery, QueryClass.Select, stats)

        statsCollectingQuery.select { _ => 1 } mustEqual List(1)

        stats.counts("db-select-count") mustEqual 1
        //stats.times("db-timing") mustEqual latency.toMillis
      //}
    }

    "collect timeout stats" in {
     //? Time.withCurrentTimeFrozen { time =>
        val stats = new FakeStatsCollector
        val testQuery = new FakeQuery(List(mock[ResultSet]))
        val statsCollectingQuery = new StatsCollectingQuery(testQuery, QueryClass.Select, stats)
        val e = new SqlQueryTimeoutException( D(0, SECONDS ) )

        statsCollectingQuery.select { _ => throw e } must throwA[SqlQueryTimeoutException]

        stats.counts("db-query-timeout-count") mustEqual 1
        stats.counts("db-query-" + QueryClass.Select.name + "-timeout-count") mustEqual 1
    //  }
    }
  }
}

