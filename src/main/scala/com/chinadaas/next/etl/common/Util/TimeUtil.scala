package Util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object TimeUtil {
  val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val DATEKEY_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  val MINUTE_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
  val HOUR_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH")

  /**
    * 获取当天日期
    */
  def getTodayDate: String = synchronized {
    DATE_FORMAT.format(new Date)
  }

  /**
    * 获得当前月
    */

  def getNowMonth: String = {
    Calendar.MONTH.toString
  }

  /**
    * 得到当前年
    */

  def getNowYear: String = {
    Calendar.YEAR.toString
  }

  /**
    * 指定年前的日期 yyyy-MM-dd
    */

  def getYearsAgo(year: Int): String = {
    val calendar = Calendar.getInstance()

    calendar.add(Calendar.YEAR, -year)

    DATE_FORMAT.format(calendar.getTime)
  }

  /**
    *
    */
}

