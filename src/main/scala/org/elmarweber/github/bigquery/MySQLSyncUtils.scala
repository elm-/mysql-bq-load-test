package org.elmarweber.github.bigquery

import javax.sql.DataSource

import org.apache.commons.dbutils.DbUtils

object MySQLSyncUtils {
  def getPrimaryIdCols(database: String, table: String)(implicit ds: DataSource): List[String] = {
    val conn = ds.getConnection()
    try {
      val rs = conn.getMetaData.getIndexInfo(database, null, table, true, false)
      try {
        var primaryIndexColumns: List[String] = Nil
        while (rs.next()) {
          val indexName = rs.getString("INDEX_NAME")
          val colName = rs.getString("COLUMN_NAME")
          if (indexName == "PRIMARY") {
            primaryIndexColumns :::= List(colName)
          }
        }
        primaryIndexColumns.sorted
      } finally {
        DbUtils.closeQuietly(rs)
      }
    } finally {
      DbUtils.closeQuietly(conn)
    }
  }
}
