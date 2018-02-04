package org.elmarweber.github

import spray.json._

package object bigquery {
  object BqTypes {
    val Integer = "integer"
    val Boolean = "boolean"
    val String = "string"
    val Float = "float"
    val Timestamp = "timestamp"
  }

  case class BqSchemaField(name: String, `type`: String = "string", mode: String = "nullable", fields: List[BqSchemaField] = Nil)
  type BqSchema = List[BqSchemaField]
  object BqSchemaField {
    implicit val BqSchemaFieldFormat = new RootJsonWriter[List[BqSchemaField]] {
      override def write(objs: List[BqSchemaField]) = {
        JsArray(objs.toVector.map(obj => JsObject(
          "name" -> JsString(obj.name),
          "type" -> JsString(obj.`type`),
          "mode" -> JsString(obj.mode),
          "fields" -> write(obj.fields))))
      }
    }
  }
}
