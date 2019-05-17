package org.mikelionis.lukas.vqf
package serialization

import query._
import similarity._
import model._

import spray.json._

import java.io.File
import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter

object JsonProtocol extends DefaultJsonProtocol {
  implicit object TableJsonFormat extends RootJsonFormat[Table] {
    override def read(json: JsValue): Table = json match {
      case JsString(value) => Table(value)
      case _ => throw DeserializationException(s"JsString expected, got '${json.getClass.getName}'")
    }

    override def write(obj: Table): JsValue = JsString(obj.fqTableName)
  }

  implicit object ColumnJsonFormat extends RootJsonFormat[Column] {
    override def read(json: JsValue): Column = json match {
      case JsString(value) => Column(value)
      case _ => throw DeserializationException(s"JsString expected, got '${json.getClass.getName}'")
    }

    override def write(obj: Column): JsValue = JsString(obj.fqColumnName)
  }

  implicit object ClauseJsonFormat extends RootJsonFormat[Clause] {
    override def read(json: JsValue): Clause = json match {
      case JsString(value) => Clause.fromString(value)
      case _ => throw DeserializationException(s"JsString expected, got '${json.getClass.getName}'")
    }

    override def write(obj: Clause): JsValue = JsString(obj.toString)
  }

  implicit object StatementJsonFormat extends RootJsonFormat[Statement] {
    override def read(json: JsValue): Statement = json match {
      case JsString(value) => Statement.fromString(value)
      case _ => throw DeserializationException(s"JsString expected, got '${json.getClass.getName}'")
    }

    override def write(obj: Statement): JsValue = JsString(obj.toString)
  }

  implicit object AnalysedQueryJsonFormat extends RootJsonFormat[AnalysedQuery] {
    override def read(json: JsValue): AnalysedQuery = json match {
      case JsObject(fields) =>
        val keys = Array("statement", "tables", "lineage", "columns", "joins")
        keys.flatMap(fields.get) match {
          case Array(statement, tables, lineage, columns, joins) =>
            AnalysedQuery(
              statement.convertTo[Statement],
              tables.convertTo[Set[Table]],
              lineage.convertTo[Set[Lineage]],
              columns.convertTo[Set[ColumnWithClause]],
              joins.convertTo[Set[Join]]
            )
          case _ =>
            throw DeserializationException(s"Incorrect values or missing required keys ('${keys.mkString(",")}')")
        }
      case _ => throw DeserializationException(s"JsObject expected, got '${json.getClass.getName}'")
    }

    override def write(obj: AnalysedQuery): JsValue = JsObject(
      "statement" -> obj.statement.toJson,
      "tables" -> obj.tables.toJson,
      "lineage" -> obj.lineage.toJson,
      "columns" -> obj.columns.toJson,
      "joins" -> obj.joins.toJson
    )
  }

  implicit object QueryMetadataJsonFormat extends RootJsonFormat[QueryMetadata] {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    private def parseDateTime(dateTime: String): LocalDateTime = LocalDateTime.parse(dateTime.take(19), formatter)

    override def read(json: JsValue): QueryMetadata = json match {
      case JsObject(fields) =>
        val keys = Array("user_name", "epoch", "session_id", "query_start", "query_duration")
        keys.flatMap(fields.get) match {
          case Array(JsString(user), JsNumber(epoch), JsString(session), JsString(start), JsNumber(duration)) =>
            QueryMetadata(user, epoch.toInt, session, parseDateTime(start), Duration.ofMillis(duration.toLong))
          case _ =>
            throw DeserializationException(s"Incorrect values or missing required keys ('${keys.mkString(",")}')")
        }
      case _ => throw DeserializationException(s"JsObject expected, got '${json.getClass.getName}'")
    }

    override def write(obj: QueryMetadata): JsValue = JsObject(
      "user_name" -> JsString(obj.user),
      "epoch" -> JsNumber(obj.epoch),
      "session_id" -> JsString(obj.session),
      "query_start" -> JsString(obj.start.format(formatter)),
      "query_duration" -> JsNumber(obj.duration.toMillis)
    )
  }

  implicit object FileJsonFormat extends RootJsonFormat[File] {
    override def read(json: JsValue): File = json match {
      case JsString(filePath) => new File(filePath)
      case _ => throw DeserializationException(s"JsString expected, got '${json.getClass.getName}'")
    }
    override def write(obj: File): JsValue = JsString(obj.getPath)
  }

  implicit object QuerySimilarityMethodFormat extends RootJsonFormat[QuerySimilarityMethod] {
    override def read(json: JsValue): QuerySimilarityMethod = json match {
      case JsObject(fields) =>
        fields("method") match {
          case JsString(className) =>
            if (className == classOf[AligonMethod].getName) {
              val weights = fields("weights").convertTo[Weights]
              new AligonMethod(weights)
            } else throw DeserializationException(s"unsupported query similarity method '$className'")
          case field => throw DeserializationException(s"JsString expected, got '${field.getClass.getName}'")
        }
      case _ => throw DeserializationException(s"JsObject expected, got '${json.getClass.getName}'")
    }

    override def write(obj: QuerySimilarityMethod): JsValue = obj match {
      case m: AligonMethod => JsObject("method" -> JsString(m.getClass.getName), "weights" -> m.weights.toJson)
      case other => throw new IllegalArgumentException(s"serialization of $other is not supported")
    }
  }

  implicit val columnWithClauseFormat: RootJsonFormat[ColumnWithClause] = jsonFormat2(ColumnWithClause.apply)
  implicit val lineageFormat: RootJsonFormat[Lineage] = jsonFormat2(Lineage.apply)
  implicit val joinFormat: RootJsonFormat[Join] = jsonFormat2(Join.apply)
  implicit val queryFormat: RootJsonFormat[Query] = jsonFormat3(Query.apply)
  implicit val weightsFormat: RootJsonFormat[Weights] = jsonFormat3(Weights.apply)
  implicit val markovModelDataFormat: RootJsonFormat[MarkovModelData] = jsonFormat3(MarkovModelData.apply)
}
