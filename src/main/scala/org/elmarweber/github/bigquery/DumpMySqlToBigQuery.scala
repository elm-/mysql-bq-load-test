package org.elmarweber.github.bigquery

import java.io.{File, FileOutputStream}
import java.sql.{ResultSet, ResultSetMetaData, SQLType, Types}
import javax.sql.DataSource
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl._
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.dbutils.{QueryRunner, ResultSetHandler}
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
  val dataFile = s"${config.outDir.getAbsolutePath}/${config.table}.json"

  implicit val system = ActorSystem("sql-bq")
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()

  val runner = new QueryRunner(config.dataSource)

  // head table to get col layout
  val schema = runner.query(s"select * from ${config.table}", new ResultSetHandler[BqSchema] {
    override def handle(rs: ResultSet): BqSchema = {
      (1 to rs.getMetaData.getColumnCount).toList.map { i =>
        val colName = rs.getMetaData.getColumnName(i)
        val nullable = if (rs.getMetaData.isNullable(i) == ResultSetMetaData.columnNullable) "nullable" else "required"
        val bqType = rs.getMetaData.getColumnType(i) match {
          case Types.BOOLEAN => BqTypes.Boolean
          case Types.INTEGER => BqTypes.Integer
          case Types.DECIMAL => BqTypes.Float
          case Types.TIMESTAMP | Types.DATE => BqTypes.Timestamp
          case Types.CHAR | Types.VARCHAR => BqTypes.String
          case o =>
            logger.warn(s"Unhandled type ${o}/${rs.getMetaData.getColumnTypeName(i)}, defaulting to string")
            BqTypes.String
        }
        BqSchemaField(colName, bqType, nullable)
      }
    }
  })
  val schemaWithIndex = schema.zipWithIndex


  val fout = new FileOutputStream(new File(dataFile))
  val counter = new AtomicLong(0)

  val (queue, future) = Source
    .queue[JsObject](512, OverflowStrategy.backpressure)
    .map { rowJso =>
      val line = (rowJso.toString + "\n").getBytes(StandardCharsets.UTF_8)
      fout.write(line)
      if (counter.incrementAndGet() % 10000 == 0) logger.info(s"Done ${counter.get()} rows")
    }
    .toMat(Sink.ignore)(Keep.both)
    .run()

  runner.query(s"select * from ${config.table}", new ResultSetHandler[Unit] {
    override def handle(rs: ResultSet): Unit = {
      try {
        while (rs.next()) {
          val rowValues = schemaWithIndex.map { case (field, i) =>
            val sqlIndex = i + 1
            val value = field.`type` match {
              case BqTypes.Boolean => JsBoolean(rs.getBoolean(sqlIndex))
              case BqTypes.Integer | BqTypes.Float => JsNumber(rs.getBigDecimal(sqlIndex))
              case BqTypes.Timestamp => JsNumber(rs.getTimestamp(sqlIndex).getTime)
              case BqTypes.String => JsString(rs.getString(sqlIndex))
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
    }
  })

  future.onComplete {
    case Success(_) =>
      fout.flush()
      fout.close()
      logger.info(s"Dumped ${counter.get()} rows to ${dataFile}")
      Files.write(Paths.get(schemaFile), schema.toJson.toString.getBytes(StandardCharsets.UTF_8))
      logger.info(s"Dumped schema to ${schemaFile}")
      System.exit(0)
    case Failure(ex) =>
      logger.error(s"Failed stream: ${ex.getMessage}", ex)
      System.exit(1)
  }
}