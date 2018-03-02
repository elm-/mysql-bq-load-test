package org.elmarweber.github.bigquery

import java.text.SimpleDateFormat
import java.util.Date

trait BigQueryUtil {
  def formatDate(timestamp: Long): String = formatDate(new Date(timestamp))

  def formatDate(date: Date): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss")
    df.format(date)
  }
}

object BigQueryUtil extends BigQueryUtil
