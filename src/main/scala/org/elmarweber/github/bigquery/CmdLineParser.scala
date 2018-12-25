package org.elmarweber.github.bigquery

import java.io.File
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, Period, ZoneOffset}
import java.time.temporal.{ChronoUnit, TemporalUnit}
import javax.sql.DataSource

import org.apache.commons.dbcp2.BasicDataSource

import scala.util.{Failure, Success, Try}

object CmdLineParser {
  private val CmdLineDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  case class Config(
    dbUrl: String = "jdbc:mysql://localhost:3306/test",
    username: String = "root",
    password: String = "",
    tables: List[String] = Nil,
    outDir: File = new File("./"),
    compress: Boolean = false,
    splitLines: Option[Int] = None,
    parallelism: Int = 1,
    generateReplId: Boolean = true,
    incrementalColumn: Option[String] = None,
    incrementalTimestamp: Option[Long] = None
  ) {
    def dataSource = {
      val ds = new BasicDataSource()
      ds.setDriverClassName("com.mysql.cj.jdbc.Driver")
      ds.setUrl(dbUrl)
      ds.setUsername(username)
      ds.setPassword(password)
      ds.setValidationQuery("select 1 from dual")
      ds
    }
  }

  val parser = new scopt.OptionParser[Config]("mysql-bq-load-test") {
    head("mysql-bq-load-test")

    opt[String]('d', "database-url")
      .required()
      .action( (x, c) => c.copy(dbUrl = x) )
      .text(s"the URL, e.g. 'jdbc:mysql://localhost:3306/test'")

    opt[String]('u', "username")
      .action( (x, c) => c.copy(username = x) )

    opt[String]('p', "password")
      .action( (x, c) => c.copy(password = x) )

    opt[Boolean]('c', "compress")
      .action( (x, c) => c.copy(compress = x) )
      .text(s"weather to compress the output with gzip")

    opt[Boolean]("generate-repl-id")
      .action( (x, c) => c.copy(generateReplId = x) )
      .text(s"weather to generate the replication id column automatically (default = true)")

    opt[Int]('s', "split")
      .action( (x, c) => c.copy(splitLines = Some(x)) )
      .text(s"the number of lines to split files after")

    opt[Int]("parallelism")
      .action( (x, c) => c.copy(parallelism = x) )
      .text(s"the number of parallel extractors to run, only makes sense if multiple tables are specified")

    opt[String]("incremental-column")
      .action( (x, c) => c.copy(incrementalColumn = Some(x)) )
      .text(s"a column to use for incremental loads, if not specified the whole table is dumped, also")
      .text(s"requires incremental-timestamp parameter, as otherwise 01.01.1970 00:00:00 is assumed as default")

    opt[String]("incremental-timestamp")
      .validate { x =>
        Try(CmdLineDateFormat.parse(x)) match {
          case Success(_) => Right(())
          case Failure(ex) =>
            if (x.endsWith("h")) {
              val substr = x.substring(0, x.lastIndexOf('h'))
              Try(substr.toLong) match {
                case Success(hours) =>
                  Right(())
                case Failure(ex) =>
                  Left(ex.getMessage)
              }
            } else {
              Left(ex.getMessage)
            }
        }
      }
      .action( (x, c) => {
        Try(CmdLineDateFormat.parse(x)) match {
          case Success(date) =>
            c.copy(incrementalTimestamp = Some(date.getTime))
          case Failure(_) =>
            val substr = x.substring(0, x.lastIndexOf('h'))
            c.copy(incrementalTimestamp = Some(LocalDateTime.now().minus(substr.toLong, ChronoUnit.HOURS).toInstant(ZoneOffset.UTC).toEpochMilli))

        }
      })
      .text(s"the date to use as incremental import (yyyy-MM-dd HH:mm:ss), can also be something in hours like 24h")

    opt[Seq[String]]('t', "tables")
      .required()
      .action( (x, c) => c.copy(tables = x.toList) )

    opt[File]('o', "out")
      .required()
      .action( (x, c) => c.copy(outDir = x) )
      .validate(f => if (f.exists()) success else failure(s"Directory ${f.getAbsolutePath} does not exist"))
  }
}
