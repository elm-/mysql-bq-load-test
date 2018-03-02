package org.elmarweber.github.bigquery

import java.io.{File, FileOutputStream}
import java.sql.{ResultSet, ResultSetMetaData, SQLType, Types}
import javax.sql.DataSource
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.GZIPOutputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl._
import com.mysql.cj.jdbc.util.ResultSetUtil
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.dbutils.{DbUtils, QueryRunner, ResultSetHandler}
import spray.json._

import scala.concurrent.Await
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object DumpMySqlToBigQuery extends App with StrictLogging {
  import CmdLineParser._

  val config = parser.parse(args, Config()) match {
    case Some(config) => config
    case None =>
      sys.exit(1)
      throw new IllegalStateException("Should not happen")
  }

  logger.info(s"Starting dump of ${config.table} at ${config.dbUrl} using ${config.username}")

  val writeTimeout = 30.seconds // timeout in case issue with slow writer

  val schemaFile = s"${config.outDir.getAbsolutePath}/${config.table}.bqschema"
  def genDataFile(i: Int) = s"${config.outDir.getAbsolutePath}/${config.table}.${i}.json${if (config.compress) ".gz" else ""}"

  implicit val system = ActorSystem("sql-bq")
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()

  val runner = new QueryRunner(config.dataSource)

  // head table to get col layout
  val schema = runner.query(s"select * from ${config.table} limit 1", new ResultSetHandler[BqSchema] {
    override def handle(rs: ResultSet): BqSchema = {
      (1 to rs.getMetaData.getColumnCount).toList.map { i =>
        val colName = rs.getMetaData.getColumnName(i)
        val bqType = rs.getMetaData.getColumnType(i) match {
          case Types.BOOLEAN => BqTypes.Boolean
          case Types.INTEGER | Types.BIGINT | Types.TINYINT | Types.BIT => BqTypes.Integer
          case Types.DECIMAL => BqTypes.Float
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
  val schemaWithIndex = schema.zipWithIndex
  logger.info(s"Successfully build schema with ${schema.size} columns")


  var fileCount = 0
  def recreateOut(i: Int) = {
    if (config.compress) {
      new GZIPOutputStream(new FileOutputStream(new File(genDataFile(i))))
    } else {
      new FileOutputStream(new File(genDataFile(i)))
    }
  }

  var out = recreateOut(fileCount)
  val counter = new AtomicLong(0)

  val (queue, future) = Source
    .queue[JsObject](512, OverflowStrategy.backpressure)
    .map { rowJso =>
      val count = counter.incrementAndGet()
      if (((count < 1000000) && count % 10000 == 0) || (count % 100000 == 0)) logger.info(s"Done ${counter.get()} rows")
      if (config.splitLines.isDefined && count % config.splitLines.get == 0) {
        out.flush()
        out.close()
        fileCount += 1
        logger.info(s"Creating new file with index ${fileCount}")
        out = recreateOut(fileCount)
      }
      val line = (rowJso.toString + "\n").getBytes(StandardCharsets.UTF_8)
      out.write(line)
    }
    .toMat(Sink.ignore)(Keep.both)
    .run()

  // manually create connection to fine tune fetch settings, settings below are required idiomatic settings for JDBC streaming behavior on MySQL
  // https://stackoverflow.com/questions/6942336/mysql-memory-ram-usage-increases-while-using-resultset
  val conn = config.dataSource.getConnection()
  val pstmt = conn.prepareStatement(s"select * from ${config.table}", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)
  pstmt.setFetchSize(Integer.MIN_VALUE)
  val rs = pstmt.executeQuery()

  try {
    while (rs.next()) {
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
                  JsNumber(rs.getTimestamp(sqlIndex).getTime)
                }
              case BqTypes.String => JsString(rs.getString(sqlIndex))
            }
          }
        }
        field.name -> value
      }
      val rowJso = JsObject(rowValues.toMap)
      Await.result(queue.offer(rowJso), writeTimeout)
    }
    queue.complete()
  } catch {
    case ex: Exception => queue.fail(ex)
  }


  def cleanSql() = {
    DbUtils.closeQuietly(rs)
    DbUtils.closeQuietly(pstmt)
    DbUtils.closeQuietly(conn)
  }

  future.onComplete {
    case Success(_) =>
      cleanSql()
      out.flush()
      out.close()
      logger.info(s"Dumped ${counter.get()} rows to ${fileCount + 1} data files")
      Files.write(Paths.get(schemaFile), schema.toJson.toString.getBytes(StandardCharsets.UTF_8))
      logger.info(s"Dumped schema to ${schemaFile}")
      System.exit(0)
    case Failure(ex) =>
      cleanSql()
      logger.error(s"Failed stream: ${ex.getMessage}", ex)
      System.exit(1)
  }
}