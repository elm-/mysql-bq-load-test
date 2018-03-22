package org.elmarweber.github.bigquery

import java.sql.{ResultSet, ResultSetMetaData, Types}
import javax.sql.DataSource

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.dbutils.{QueryRunner, ResultSetHandler}

/** Builds a BqSChema from an SQL table */
trait BqSchemaBuilder extends StrictLogging {
  def buildSchema(table: String)(implicit ds: DataSource): BqSchema = {
    val runner = new QueryRunner(ds)

    // head table to get columns
    runner.query(s"select * from ${table} limit 1", new ResultSetHandler[BqSchema] {
      override def handle(rs: ResultSet): BqSchema = {
        (1 to rs.getMetaData.getColumnCount).toList.map { i =>
          val colName = rs.getMetaData.getColumnName(i)
          val bqType = rs.getMetaData.getColumnType(i) match {
            case Types.BOOLEAN => BqTypes.Boolean
            case Types.INTEGER | Types.BIGINT | Types.SMALLINT | Types.TINYINT | Types.BIT => BqTypes.Integer
            case Types.DECIMAL | Types.FLOAT | Types.REAL | Types.DOUBLE => BqTypes.Float
            case Types.TIMESTAMP | Types.DATE => BqTypes.Timestamp
            case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR => BqTypes.String
            case o =>
              logger.warn(s"Unhandled type ${o}/${rs.getMetaData.getColumnTypeName(i)}, defaulting to string")
              BqTypes.String
          }
          // timestamps are always nullable because
          val nullable = if ((rs.getMetaData.isNullable(i) == ResultSetMetaData.columnNullable) || bqType == BqTypes.Timestamp) "nullable" else "required"

          BqSchemaField(colName, bqType, nullable)
        }
      }
    })
  }
}

object BqSchemaBuilder extends BqSchemaBuilder
