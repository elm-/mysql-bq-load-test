package org.elmarweber.github.bigquery

import java.io.{File, FileOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{ResultSet, ResultSetMetaData, Types}
import java.util.zip.GZIPOutputStream
import javax.sql.DataSource

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.dbutils.{QueryRunner, ResultSetHandler}
import org.elmarweber.github.bigquery.DumpMySqlToBigQuery.logger
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

/**
  * A stream that queries a MySQL table and outputs the schema and rows as JSON data files.
  */
trait DumpMySqlTableStream extends StrictLogging with DefaultJsonProtocol {
  private val ReplIdColName = "repl_id"
  private val ReplIdBqSchemaCol = BqSchemaField("repl_id", BqTypes.String)

  def genSchemaFilename(table: String): String
  def genDataFilename(table: String, fileCount: Int, compress: Boolean): String


  private def createOut(table: String, i: Int, compress: Boolean) = {
    if (compress) {
      new GZIPOutputStream(new FileOutputStream(new File(genDataFilename(table, i, true))))
    } else {
      new FileOutputStream(new File(genDataFilename(table, i, false)))
    }
  }

  private def logCount(table: String, count: Int) = {
    if (count > 0 && (((count < 1000000) && count % 10000 == 0) || (count % 100000 == 0))) logger.info(s"${table}: Done ${count} rows")
  }


  def createStream(table: String, compress: Boolean, splitLines: Option[Int])(implicit ds: DataSource, mat: Materializer, ec: ExecutionContext): Future[Int]  = {
    val tableSchema = BqSchemaBuilder.buildSchema(table)
    val bqSchema = tableSchema ::: List(ReplIdBqSchemaCol)
    val primaryIdCols = MySQLSyncUtils.getPrimaryIdCols(ds.asInstanceOf[BasicDataSource].getDefaultCatalog, table)
    logger.info(s"${table}: Successfully build schema with ${tableSchema.size} columns and ${primaryIdCols.mkString(", ")} as primary ID cols")


    Source
      .fromGraph(SqlReaderToJsonSource.create(table, tableSchema)(ds))
      .map { rowJso =>
        val replId = primaryIdCols.map { col =>
          rowJso.fields(col) match {
            case JsNull => "null"
            case JsString(value) => value
            case JsNumber(value) => value.toString
            case JsBoolean(value) => value.toString
          }
        }.mkString("_")
        val rowJsoWithReplId = JsObject(rowJso.fields ++ Map(ReplIdColName -> JsString(replId)))
        val line = (rowJsoWithReplId.toString + "\n").getBytes(StandardCharsets.UTF_8)
        line
      }
      .runWith(Sink.fold((None: Option[OutputStream], 0, 0)) { case ((outOpt, lineCount, fileCount), lineData) =>
        logCount(table, lineCount)
        val (out, newFileCount) = outOpt match {
          case None => (createOut(table, fileCount, compress), fileCount)
          case Some(out) if splitLines.isDefined && lineCount % splitLines.get == 0  =>
            out.flush()
            out.close()
            val newFileCount = fileCount + 1
            logger.info(s"${table}: Creating new file with index ${newFileCount}")
            (createOut(table, newFileCount, compress), newFileCount)
          case Some(out) => (out, fileCount)
        }
        out.write(lineData)
        (Some(out), lineCount + 1, newFileCount)
      })
      .map { case (outOpt, lineCount, fileCount) =>
        outOpt.foreach { out =>
          out.flush()
          out.close()
        }
        logger.info(s"${table}: Dumped ${lineCount} rows to ${fileCount + 1} data files")
        if (lineCount == 0) {
          logger.info("Creating dummy output file because no file was written so far")
          val fout = createOut(table, fileCount, compress)
          fout.close()
        }
        Files.write(Paths.get(genSchemaFilename(table)), bqSchema.toJson.toString.getBytes(StandardCharsets.UTF_8))
        logger.info(s"${table}: Dumped schema to ${genSchemaFilename(table)}")
        lineCount
      }
  }
}
