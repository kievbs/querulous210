package com.twitter.querulous.unit

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import com.twitter.querulous.database.{Database, DatabaseFactory, MemoizingDatabaseFactory}

class MemoizingDatabaseFactorySpec extends Specification with Mockito {
  val username = "username"
  val password = "password"
  val hosts = List("foo")

  "MemoizingDatabaseFactory" should {
    "apply" in {
      val database1 = mock[Database]
      val database2 = mock[Database]
      val databaseFactory = mock[DatabaseFactory]
      val memoizingDatabase = new MemoizingDatabaseFactory(databaseFactory)

      databaseFactory.apply(hosts, "bar", username, password, Map.empty, Database.DEFAULT_DRIVER_NAME) returns database1
      databaseFactory.apply(hosts, "baz", username, password, Map.empty, Database.DEFAULT_DRIVER_NAME) returns database2

      /*expect {
        one(databaseFactory).apply(hosts, "bar", username, password, Map.empty, Database.DEFAULT_DRIVER_NAME) willReturn database1
        one(databaseFactory).apply(hosts, "baz", username, password, Map.empty, Database.DEFAULT_DRIVER_NAME) willReturn database2
      }*/
      memoizingDatabase(hosts, "bar", username, password) mustEqual database1
      memoizingDatabase(hosts, "bar", username, password) mustEqual database1
      memoizingDatabase(hosts, "baz", username, password) mustEqual database2
      memoizingDatabase(hosts, "baz", username, password) mustEqual database2
    }

    "not cache" in {
      val database = mock[Database]
      val factory = mock[DatabaseFactory]
      val memoizingDatabase = new MemoizingDatabaseFactory(factory)

      factory.apply(hosts, username, password) returns database
      //expect {
        //exactly(2).of(factory).apply(hosts, username, password) willReturn database
      //}

      memoizingDatabase(hosts, username, password) mustEqual database
      memoizingDatabase(hosts, username, password) mustEqual database
    }
  }
}
