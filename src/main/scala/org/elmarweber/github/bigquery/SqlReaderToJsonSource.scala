package org.elmarweber.github.bigquery


import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}
import javax.sql.DataSource

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.dbutils.DbUtils
import spray.json._


/**
  * Akka stream source implementation with backpressure support that streams a MySQL query into BqSchema compatible
  * json objects.
  */
class SqlReaderToJsonSource(table: String, bqSchema: BqSchema, incrementalColumn: Option[String], incrementalTimestamp: Option[Long])(implicit ds: DataSource) extends GraphStage[SourceShape[JsObject]] {
  private val out: Outlet[JsObject] = Outlet(s"${classOf[SqlReaderToJsonSource].getSimpleName}-${table}")
  private val schemaWithIndex = bqSchema.zipWithIndex

  override def shape: SourceShape[JsObject] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes) = {
    new SqlReaderSourceStageLogic()
  }


  class SqlReaderSourceStageLogic extends GraphStageLogic(shape) with StrictLogging {
    case class QueryState(conn: Connection, pstmt: PreparedStatement, rs: ResultSet)

    private var state: Option[QueryState] = None

    private def startQuery() = {
      // manually create connection to fine tune fetch settings, settings below are required idiomatic settings for JDBC streaming behavior on MySQL
      // https://stackoverflow.com/questions/6942336/mysql-memory-ram-usage-increases-while-using-resultset
      val conn = ds.getConnection()
      try {
        val pstmt = {
          if (incrementalColumn.isDefined) {
            val stmt = conn.prepareStatement(s"select * from ${table} where ${incrementalColumn.get} >= ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)
            stmt.setTimestamp(1, new Timestamp(incrementalTimestamp.get))
            stmt
          } else {
            conn.prepareStatement(s"select * from ${table}", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)
          }
        }
        pstmt.setFetchSize(Integer.MIN_VALUE)

        try {
          val rs = pstmt.executeQuery()
          QueryState(conn, pstmt, rs)
        } catch  {
          case ex: Exception =>
            DbUtils.closeQuietly(pstmt)
            throw ex
        }
      } catch {
        case ex: Exception =>
          DbUtils.closeQuietly(conn)
          throw ex
      }
    }

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (state.isEmpty) {
          state = Some(startQuery())
        }

        assert(state.isDefined, "Query not initialized")
        val rs = state.get.rs
        if (rs.next()) {
          val rowValues = schemaWithIndex.map { case (field, i) =>
            val sqlIndex = i + 1
            val value = {
              if (rs.getString(sqlIndex) == null) {
                JsNull
              } else {
                field.`type` match {
                  case BqTypes.Boolean => JsBoolean(rs.getBoolean(sqlIndex))
                  case BqTypes.Integer | BqTypes.Float => JsNumber(rs.getBigDecimal(sqlIndex))
                  case BqTypes.Timestamp =>
                    // have to re-check null due to special TS handling with convert to null param in connection string
                    if (rs.getTimestamp(sqlIndex) == null) {
                      JsNull
                    } else {
                      JsString(BigQueryUtil.formatDate(rs.getTimestamp(sqlIndex).getTime))
                    }
                  case BqTypes.String => JsString(rs.getString(sqlIndex))
                }
              }
            }
            field.name -> value
          }
          val rowJso = JsObject(rowValues.toMap)
          push(out, rowJso)
        } else {
          completeStage()
        }
      }
    })


    override def postStop(): Unit = {
      if (state.isDefined) {
        DbUtils.closeQuietly(state.get.rs)
        DbUtils.closeQuietly(state.get.pstmt)
        DbUtils.closeQuietly(state.get.conn)
      }
    }
  }
}

object SqlReaderToJsonSource {
  def create(table: String, bqSchema: BqSchema, incrementalColumn: Option[String], incrementalTimestamp: Option[Long])(implicit ds: DataSource) =
    new SqlReaderToJsonSource(table, bqSchema, incrementalColumn, incrementalTimestamp)
}

