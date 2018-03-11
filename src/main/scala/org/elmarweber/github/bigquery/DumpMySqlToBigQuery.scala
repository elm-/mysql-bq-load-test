package org.elmarweber.github.bigquery

import java.io.{File, FileOutputStream, OutputStream}
import java.sql.{ResultSet, ResultSetMetaData, SQLType, Types}
import javax.sql.DataSource
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Date
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

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object DumpMySqlToBigQuery extends App with StrictLogging with DumpMySqlTableStream {
  import CmdLineParser._

  val config = parser.parse(args, Config()) match {
    case Some(config) => config
    case None =>
      sys.exit(1)
      throw new IllegalStateException("Should not happen")
  }

  implicit def ds = config.dataSource

  logger.info(s"Starting dumps of ${config.tables.size} tables from ${config.dbUrl} using ${config.username}")

  // constant date prefix for all files writte during this session
  val datePrefix = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date(System.currentTimeMillis()))

  def genSchemaFilename(table: String) = s"${config.outDir.getAbsolutePath}/${table}.bqschema"
  def genDataFilename(table: String, fileCount: Int, compress: Boolean) = {
    s"${config.outDir.getAbsolutePath}/${datePrefix}_${table}.${fileCount}.json${if (compress) ".gz" else ""}"
  }

  implicit val system = ActorSystem("sql-bq")
  implicit val ec = system.dispatcher
  implicit val mmat = ActorMaterializer()


  val future = Source
    .fromIterator(() => config.tables.iterator)
    .mapAsyncUnordered(config.parallelism) { table =>
      createStream(table, config.compress, config.splitLines).map { lineCount =>
        (table, lineCount)
      }
    }
    .runWith(Sink.seq)


  future.onComplete {
    case Success(tableResults) =>
      tableResults.foreach {
        case (table, lineCount) => logger.info(s"Dumped for table ${table} ${lineCount} rows")
      }
      val totalLines = tableResults.map(_._2).sum
      logger.info(s"Dumped ${totalLines} rows in total")
      System.exit(0)
    case Failure(ex) =>
      logger.error(s"Failed stream: ${ex.getMessage}", ex)
      System.exit(1)
  }
}