package com.twitter.querulous

import compat.Platform
import concurrent.{ Future, ExecutionContext }

trait StatsCollector {
  def incr(name: String, count: Int)
  def time[A](name: String)(f: => A): A
  def addGauge(name: String)(gauge: => Double) {}
  def addMetric(name: String, value: Int) {}

  def timeFutureMillis[T](name: String)(f: Future[T])( implicit ctx :ExecutionContext ) = {
    val start = Platform.currentTime
    f onComplete { _ =>
      addMetric(name +"_msec", (Platform.currentTime - start).toInt)
    }
  }
}

object NullStatsCollector extends StatsCollector {
  def incr(name: String, count: Int) {}
  def time[A](name: String)(f: => A): A = f
  override def timeFutureMillis[T](name: String)(f: Future[T])(implicit ctx :ExecutionContext) = f
}
