package io.github.quzhengpeng.tools

import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat

object DateUtils extends Utils {

  def str2date(str: String, format: String = "yyyy-MM-dd"): DateTime = {
    DateTime.parse(str, DateTimeFormat.forPattern(format))
  }

  def getBeforeDays(date: DateTime, n: Int = 1): DateTime = {
    date.plusDays(-n)
  }

  def getAfterDays(date: DateTime, n: Int = 1): DateTime = {
    date.plusDays(n)
  }

  def getDaysDiff(date1: DateTime, date2: DateTime): Long = {
    Days.daysBetween(date1, date2).getDays
  }

}
