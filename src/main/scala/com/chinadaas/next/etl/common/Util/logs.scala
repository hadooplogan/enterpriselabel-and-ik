package Util

import org.apache.log4j._

trait logs {
  private[this] val logger = Logger.getLogger(getClass().getName())

  import org.apache.log4j.Level._

  //这样传递参数，调用方法时不会立刻计算，t: => Long delayed methos
  def debug(message: => String) = if (logger.isEnabledFor(DEBUG)) logger.debug(message)

  def debug(message: => String, ex: Throwable) = if (logger.isEnabledFor(DEBUG)) logger.debug(message, ex)

  def debugValue[T](valueName: String, value: => T): T = {
    val result: T = value
    debug(valueName + "==" + result.toString)
    result
  }

  def info(message: => String) = if (logger.isEnabledFor(INFO)) logger.info(message)

  def info(message: => String, ex: Throwable) = if (logger.isEnabledFor(INFO)) logger.info(message, ex)

  def warn(message: => String) = if (logger.isEnabledFor(WARN)) logger.warn(message)

  def warn(message: => String, ex: Throwable) = if (logger.isEnabledFor(WARN)) logger.warn(message, ex)

  def erro(ex: Throwable) = if (logger.isEnabledFor(ERROR)) logger.error(ex.toString)

  def erro(message: => String) = if (logger.isEnabledFor(ERROR)) logger.error(message)

  def erro(message: => String, ex: Throwable) = if (logger.isEnabledFor(ERROR)) logger.warn(message, ex)


  def fatal(ex: Throwable) = if (logger.isEnabledFor(FATAL)) logger.fatal(ex.toString)

  def fatal(message: => String) = if (logger.isEnabledFor(FATAL)) logger.fatal(message)

  def fatal(message: => String, ex: Throwable) = if (logger.isEnabledFor(FATAL)) logger.fatal(message, ex)


}

