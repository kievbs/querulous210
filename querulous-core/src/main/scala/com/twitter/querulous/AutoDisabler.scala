package com.twitter.querulous

import concurrent.duration._
import compat.Platform
import java.sql.SQLException

trait AutoDisabler {
  protected val disableErrorCount: Int
  protected val disableDuration: Duration

  //private var disabledUntil: Time = Time.epoch
  private var disabledUntil :Long = Platform.currentTime //Long.MaxValue
  private var consecutiveErrors = 0

  protected def throwIfDisabled(throwMessage: String): Unit = {
    synchronized {
      if (Platform.currentTime < disabledUntil) {
        throw new SQLException("Server is temporarily disabled: " + throwMessage)
      }
    }
  }

  protected def throwIfDisabled(): Unit = { throwIfDisabled("") }

  protected def noteOperationOutcome(success: Boolean) {
    synchronized {
      if (success) {
        consecutiveErrors = 0
      } else {
        consecutiveErrors += 1
        if (consecutiveErrors >= disableErrorCount) {
          //disabledUntil = disableDuration.fromNow
          disabledUntil = Long.MaxValue 
        }
      }
    }
  }
}
