package com.twitter.querulous.unit

import org.specs2.mutable._
import org.specs2.mock.Mockito //{Mockito, ClassMocker}
import org.specs2.matcher._
import java.sql.{ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException
import com.twitter.querulous.test.FakeQueryEvaluator
import com.twitter.querulous.evaluator.{AutoDisablingQueryEvaluator, Transaction}
import compat.Platform
import scala.concurrent.duration.{ Duration => D }
import scala.concurrent.duration._


class AutoDisablingQueryEvaluatorSpec extends Specification with Mockito {
  sequential

  "AutoDisablingQueryEvaluator" should {
    "select" in {
      val trans = mock[Transaction]
      val disableErrorCount = 5
      val disableDuration = D(60, SECONDS) //.minute
      val queryEvaluator = new FakeQueryEvaluator(trans, List(mock[ResultSet]))
      val autoDisablingQueryEvaluator = new AutoDisablingQueryEvaluator(queryEvaluator, disableErrorCount, disableDuration)

      "when there are no failures" >> {
        autoDisablingQueryEvaluator.select("SELECT 1 FROM DUAL") { _ => 1 } mustEqual List(1)
      }

      "when there are some failures" >> {
        "when the failures are either MySQLIntegrityConstraintViolationException or SQLIntegrityConstraintViolationException" >> {
          var invocationCount = 0

          (0 until disableErrorCount + 1) foreach { i =>
            autoDisablingQueryEvaluator.select("SELECT 1 FROM DUAL") { resultSet =>
              invocationCount += 1
              throw new MySQLIntegrityConstraintViolationException
            } must throwA[MySQLIntegrityConstraintViolationException]
          }
          invocationCount mustEqual disableErrorCount + 1

          invocationCount = 0
          (0 until disableErrorCount + 1) foreach { i =>
            autoDisablingQueryEvaluator.select("SELECT 1 FROM DUAL") { resultSet =>
              invocationCount += 1
              throw new SQLIntegrityConstraintViolationException
            } must throwA[SQLIntegrityConstraintViolationException]
          }
          invocationCount mustEqual disableErrorCount + 1
        }

        "when the failures are any other exception" >> {
          "when there are more than disableErrorCount failures" >> {
            var invocationCount = 0

            (0 until disableErrorCount + 1) foreach { i =>
              autoDisablingQueryEvaluator.select("SELECT 1 FROM DUAL") { resultSet =>
                invocationCount += 1
                throw new SQLException
              } must throwA[SQLException]
            }
            invocationCount mustEqual disableErrorCount
          }

 //I don't know how to port this one 
        /*  "when there are more than disableErrorCount failures but disableDuration has elapsed" >> {
            Time.withCurrentTimeFrozen { time =>
              var invocationCount = 0

              (0 until disableErrorCount + 1) foreach { i =>
                autoDisablingQueryEvaluator.select("SELECT 1 FROM DUAL") { resultSet =>
                  invocationCount += 1
                  throw new SQLException
                } must throwA[SQLException]
              }
              invocationCount mustEqual disableErrorCount

              time.advance(1.minute)
              autoDisablingQueryEvaluator.select("SELECT 1 FROM DUAL") { resultSet =>
                invocationCount += 1
              }
              invocationCount mustEqual disableErrorCount + 1
            }
          } */
        }
      } 
    }
  }
}
