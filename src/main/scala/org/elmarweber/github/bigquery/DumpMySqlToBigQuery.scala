package org.elmarweber.github.bigquery

import java.io.{File, FileOutputStream, OutputStream}
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
          case Types.DECIMAL | Types.FLOAT => BqTypes.Float
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


  def createOut(i: Int) = {
    if (config.compress) {
      new GZIPOutputStream(new FileOutputStream(new File(genDataFile(i))))
    } else {
      new FileOutputStream(new File(genDataFile(i)))
    }
  }

  def logCount(count: Int) = {
    if (count > 0 && (((count < 1000000) && count % 10000 == 0) || (count % 100000 == 0))) logger.info(s"Done ${count} rows")
  }


  val future = Source
    .fromGraph(SqlReaderToJsonSource.create(config.table, schema)(config.dataSource))
    .map { rowJso =>
      val line = (rowJso.toString + "\n").getBytes(StandardCharsets.UTF_8)
      line
    }
    .runWith(Sink.fold((None: Option[OutputStream], 0, 0)) { case ((outOpt, lineCount, fileCount), lineData) =>
      logCount(lineCount)
      val (out, newFileCount) = outOpt match {
        case None => (createOut(fileCount), fileCount)
        case Some(out) if config.splitLines.isDefined && lineCount % config.splitLines.get == 0  =>
          out.flush()
          out.close()
          val newFileCount = fileCount + 1
          logger.info(s"Creating new file with index ${newFileCount}")
         (createOut(newFileCount), newFileCount)
        case Some(out) => (out, fileCount)
      }
      out.write(lineData)
      (Some(out), lineCount + 1, newFileCount)
    })



  future.onComplete {
    case Success((outOpt, lineCount, fileCount)) =>
      outOpt.foreach { out =>
        out.flush()
        out.close()
      }
      logger.info(s"Dumped ${lineCount} rows to ${fileCount + 1} data files")
      Files.write(Paths.get(schemaFile), schema.toJson.toString.getBytes(StandardCharsets.UTF_8))
      logger.info(s"Dumped schema to ${schemaFile}")
      System.exit(0)
    case Failure(ex) =>
      logger.error(s"Failed stream: ${ex.getMessage}", ex)
      System.exit(1)
  }
}