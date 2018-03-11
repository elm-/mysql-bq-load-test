package org.elmarweber.github.bigquery

import java.io.File
import javax.sql.DataSource

import org.apache.commons.dbcp2.BasicDataSource

object CmdLineParser {
  case class Config(
    dbUrl: String = "jdbc:mysql://localhost:3306/test",
    username: String = "root",
    password: String = "",
    tables: List[String] = Nil,
    outDir: File = new File("./"),
    compress: Boolean = false,
    splitLines: Option[Int] = None,
    parallelism: Int = 1
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

    opt[Int]('s', "split")
      .action( (x, c) => c.copy(splitLines = Some(x)) )
      .text(s"the number of lines to split files after")

    opt[Int]("parallelism")
      .action( (x, c) => c.copy(parallelism = x) )
      .text(s"the number of parallel extractors to run, only makes sense if multiple tables are specified")

    opt[Seq[String]]('t', "tables")
      .required()
      .action( (x, c) => c.copy(tables = x.toList) )

    opt[File]('o', "out")
      .required()
      .action( (x, c) => c.copy(outDir = x) )
      .validate(f => if (f.exists()) success else failure(s"Directory ${f.getAbsolutePath} does not exist"))
  }
}
