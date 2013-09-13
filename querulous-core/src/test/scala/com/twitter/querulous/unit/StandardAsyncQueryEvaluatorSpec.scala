package com.twitter.querulous.unit

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito // {ClassMocker, Mockito}
import java.sql.{Connection, ResultSet}
import scala.concurrent._
import scala.concurrent.duration.{Duration => D}
import scala.concurrent.duration._
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.evaluator.{QueryEvaluator, ParamsApplier, Transaction}
import com.twitter.querulous.database.Database
import com.twitter.querulous.query._
import com.twitter.querulous.async._
import scala.concurrent.ExecutionContext.Implicits.global

class StandardAsyncQueryEvaluatorSpec extends Specification with Mockito { // with ClassMocker {

  val to = D(1,SECONDS)

  def newEvaluator(db :Database, qf :QueryFactory ) = {
    new StandardAsyncQueryEvaluator(
      new BlockingDatabaseWrapper(db),
      //new BlockingDatabaseWrapper(1, Int.MaxValue, database),
      qf
    )
  }

  // operator functions. Declared here so that identity equality works for expectations
  val fromRow  = (r: ResultSet) => r.getInt("1")

  "BlockingEvaluatorWrapper" should {
    "select" in {
      val database     :Database     = mock[Database]
      val connection   :Connection   = mock[Connection]
      val query        :Query        = mock[Query]
      val queryFactory :QueryFactory = mock[QueryFactory]

      database.hosts                                                returns List("localhost")
      database.name                                                 returns "test"
      database.openTimeout                                          returns D(500, MILLISECONDS) 
      database.open()                                               returns connection
      queryFactory.apply(connection, QueryClass.Select, "SELECT 1") returns query
      query.select(fromRow)                                         returns Seq(1)
      
      Await.result( newEvaluator(database, queryFactory).select("SELECT 1")(fromRow), to) 

      got {
        one(database).hosts                                             
        one(database).name                                                 
        //one(database).openTimeout                                          
        one(database).open()                                               
        one(queryFactory).apply(connection, QueryClass.Select, "SELECT 1") 
        one(query).select(fromRow)                                         
      } 
    }

    "selectOne" in {
      val database     :Database     = mock[Database]
      val connection   :Connection   = mock[Connection]
      val query        :Query        = mock[Query]
      val queryFactory :QueryFactory = mock[QueryFactory]

      database.hosts                                                returns List("localhost")
      database.name                                                 returns "test"
      database.openTimeout                                          returns D(500, MILLISECONDS) 
      database.open()                                               returns connection
      queryFactory.apply(connection, QueryClass.Select, "SELECT 1") returns query
      query.select(fromRow)                                         returns Seq(1)

      Await.result( newEvaluator(database, queryFactory).selectOne("SELECT 1")(fromRow), to)      

      got {
        one(database).hosts                                             
        one(database).name                                                 
        //one(database).openTimeout                                          
        one(database).open()                                               
        one(queryFactory).apply(connection, QueryClass.Select, "SELECT 1") 
        one(query).select(fromRow)                                         
      }
    }

    "count" in {
      val database     :Database     = mock[Database]
      val connection   :Connection   = mock[Connection]
      val query        :Query        = mock[Query]
      val queryFactory :QueryFactory = mock[QueryFactory]

      database.hosts                                                returns List("localhost")
      database.name                                                 returns "test"
      database.openTimeout                                          returns D(500, MILLISECONDS) 
      database.open()                                               returns connection
      queryFactory.apply(connection, QueryClass.Select, "SELECT 1") returns query
      query.select(any[ResultSet => Int])                      returns Seq(1)
      
      Await.result( newEvaluator(database, queryFactory).count("SELECT 1"), to )

      got {

        one(database).hosts                                             
        one(database).name                                                 
        //one(database).openTimeout                                          
        one(database).open()                                               
        one(queryFactory).apply(connection, QueryClass.Select, "SELECT 1") 
        one(query).select(any[ResultSet => Int])                                          
      }
      
    }

    "execute" in {
      val database     :Database     = mock[Database]
      val connection   :Connection   = mock[Connection]
      val query        :Query        = mock[Query]
      val queryFactory :QueryFactory = mock[QueryFactory]

      val sql = "INSERT INTO foo (id) VALUES (1)"
      database.hosts                                                returns List("localhost")
      database.name                                                 returns "test"
      database.openTimeout                                          returns D(500, MILLISECONDS) 
      database.open()                                               returns connection
      queryFactory.apply(connection, QueryClass.Execute, sql)       returns query
      query.execute()                                               returns 1
      
      Await.result( newEvaluator(database, queryFactory).execute("INSERT INTO foo (id) VALUES (1)"), to )

      got {
        one(database).hosts                                             
        one(database).name                                                 
        //one(database).openTimeout                                          
        one(database).open()                                               
        one(queryFactory).apply(connection, QueryClass.Execute, sql) 
        one(query).execute()                                         
      }
    }

    "executeBatch" in {
      val database     :Database     = mock[Database]
      val connection   :Connection   = mock[Connection]
      val query        :Query        = mock[Query]
      val queryFactory :QueryFactory = mock[QueryFactory]

      val sql = "INSERT INTO foo (id) VALUES (?)"
      database.hosts                                                returns List("localhost")
      database.name                                                 returns "test"
      database.openTimeout                                          returns D(500, MILLISECONDS) 
      database.open()                                               returns connection
      queryFactory.apply(connection, QueryClass.Execute, sql)       returns query
      query.execute()                                               returns 1
      
      Await.result( newEvaluator(database, queryFactory).executeBatch("INSERT INTO foo (id) VALUES (?)")(_(1)), to )

      got {
        one(database).hosts                                             
        one(database).name                                                 
        //one(database).openTimeout                                          
        one(database).open()                                               
        one(queryFactory).apply(connection, QueryClass.Execute, sql) 
        one(query).addParams(1)
        one(query).execute()                                         
      }
    }

    "transaction" in {
      val database     :Database     = mock[Database]
      val connection   :Connection   = mock[Connection]
      val query        :Query        = mock[Query]
      val queryFactory :QueryFactory = mock[QueryFactory]

      val sql = "INSERT INTO foo (id) VALUES (1)"
      database.hosts                                                returns List("localhost")
      database.name                                                 returns "test"
      database.openTimeout                                          returns D(500, MILLISECONDS) 
      database.open()                                               returns connection
      queryFactory.apply(connection, QueryClass.Execute, sql)       returns query
      query.execute()                                               returns 1
      
      Await.result( newEvaluator(database, queryFactory).transaction(_.execute("INSERT INTO foo (id) VALUES (1)")), to)

      got {
        one(database).hosts                                             
        one(database).name                                                 
        //one(database).openTimeout                                          
        one(database).open() 
        one(connection).setAutoCommit(false)                                              
        one(queryFactory).apply(connection, QueryClass.Execute, sql) 
        one(query).execute()
        one(connection).commit()
        one(connection).setAutoCommit(true)                                         
      }
    }
  }
}
