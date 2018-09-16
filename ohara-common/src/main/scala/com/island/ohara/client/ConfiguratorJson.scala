package com.island.ohara.client

import com.island.ohara.serialization.DataType
import org.apache.commons.lang3.exception.ExceptionUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

/**
  * a collection of marshalling/unmarshalling configurator data to/from json.
  * NOTED: the json format is a part of PUBLIC INTERFACE so please don't change the field names after releasing the ohara.
  * NOTED: common data must be put on the head.
  */
object ConfiguratorJson {
  //------------------------------------------------[COMMON]------------------------------------------------//
  /**
    * Provide a way to format DataType. Both Schema and SchemaRrquest use DataType.
    */
  implicit val DATA_TYPE_JSON_FORMAT: RootJsonFormat[DataType] = new RootJsonFormat[DataType] {
    override def write(obj: DataType): JsValue = JsString(obj.name)
    override def read(json: JsValue): DataType = DataType.of(json.asInstanceOf[JsString].value)
  }
  val VERSION_V0 = "v0"
  val PRIVATE_API = "_private"
  val UNKNOWN = "?"

  //------------------------------------------------[DATA]------------------------------------------------//
  sealed trait Data {
    def uuid: String
    def name: String

    /**
      * This field should not be marshalled into json so we make it be "def" rather than "val.
      * DON'T change this filed since it is exposed by restful APIs
      * @return the type of this class
      */
    def kind: String
  }

  /**
    * used to send data command
    */
  sealed trait DataCommandFormat[T] {
    def format(address: String): String
    def format(address: String, uuid: String): String
  }

  final case class Column(name: String, typeName: DataType, order: Int)
  implicit val COLUMN_JSON_FORMAT: RootJsonFormat[Column] = new RootJsonFormat[Column] {
    override def read(json: JsValue): Column = json.asJsObject.getFields("name", "type", "order") match {
      case Seq(JsString(n), JsString(t), JsNumber(o)) => Column(n, DataType.of(t), o.toInt)
      case _                                          => throw new UnsupportedOperationException(s"invalid format of ${Column.getClass.getSimpleName}")
    }
    override def write(obj: Column): JsValue = JsObject(
      "name" -> JsString(obj.name),
      "type" -> JsString(obj.typeName.name),
      "order" -> JsNumber(obj.order)
    )
  }
  object Column {
    // kafka connector accept only Map[String, String] as input arguments so we have to serialize the column to a string
    // TODO: Personally, I hate this ugly workaround...by chia
    val COLUMN_KEY: String = "__row_connector_schema"
    def toString(schema: Seq[Column]): String =
      schema.map(c => s"${c.name},${c.typeName.name},${c.order}").mkString(",")
    def toColumns(columnsString: String): Seq[Column] = if (columnsString == null || columnsString.isEmpty) Seq.empty
    else {
      val splits = columnsString.split(",")
      if (splits.length % 3 != 0) throw new IllegalArgumentException(s"invalid format of columns string:$columnsString")
      splits
        .grouped(3)
        .map {
          case Array(name, typeName, order) => Column(name, DataType.of(typeName), order.toInt)
        }
        .toSeq
    }
  }
  //------------------------------------------------[DATA-TOPIC]------------------------------------------------//
  val TOPIC_INFO_PATH = "topics"
  final case class TopicInfoRequest(name: String, numberOfPartitions: Int, numberOfReplications: Short)
  implicit val TOPIC_INFO_REQUEST_JSON_FORMAT: RootJsonFormat[TopicInfoRequest] = jsonFormat3(TopicInfoRequest)

  final case class TopicInfo(uuid: String,
                             name: String,
                             numberOfPartitions: Int,
                             numberOfReplications: Short,
                             lastModified: Long)
      extends Data {
    override def kind: String = "topic"
  }
  implicit val TOPIC_INFO_JSON_FORMAT: RootJsonFormat[TopicInfo] = jsonFormat5(TopicInfo)
  implicit val TOPIC_INFO_COMMAND_FORMAT: DataCommandFormat[TopicInfo] = new DataCommandFormat[TopicInfo] {
    override def format(address: String): String = s"http://$address/$VERSION_V0/$TOPIC_INFO_PATH"
    override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$TOPIC_INFO_PATH/$uuid"
  }

  //------------------------------------------------[DATA-HDFS]------------------------------------------------//
  val HDFS_PATH = "hdfs"
  final case class HdfsInformationRequest(name: String, uri: String)
  implicit val HDFS_INFORMATION_REQUEST_JSON_FORMAT: RootJsonFormat[HdfsInformationRequest] = jsonFormat2(
    HdfsInformationRequest)

  final case class HdfsInformation(uuid: String, name: String, uri: String, lastModified: Long) extends Data {
    override def kind: String = "hdfs"
  }
  implicit val HDFS_INFORMATION_JSON_FORMAT: RootJsonFormat[HdfsInformation] = jsonFormat4(HdfsInformation)
  implicit val HDFS_INFORMATION_COMMAND_FORMAT: DataCommandFormat[HdfsInformation] =
    new DataCommandFormat[HdfsInformation] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$HDFS_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$HDFS_PATH/$uuid"
    }

  //------------------------------------------------[DATA-PIPELINE]------------------------------------------------//
  sealed abstract class Status extends Serializable {
    def name: String
  }

  object Status {
    case object RUNNING extends Status {
      override def name: String = "running"
    }
    case object STOPPED extends Status {
      override def name: String = "stopped"
    }

    /**
      * @return a array of all supported data type
      */
    def all = Seq(RUNNING, STOPPED)

    /**
      * seek the data type by the type name
      * @param name index of data type
      * @return Data type
      */
    def of(name: String): Status = all.find(_.name.equalsIgnoreCase(name)).get
  }

  implicit val STATUS_JSON_FORMAT: RootJsonFormat[Status] = new RootJsonFormat[Status] {
    override def write(obj: Status): JsValue = JsString(obj.name)
    override def read(json: JsValue): Status = Status.of(json.asInstanceOf[JsString].value)
  }

  val PIPELINE_PATH = "pipelines"

  /**
    * used to control data
    */
  sealed trait ControlCommandFormat[T] {
    def start(address: String, uuid: String): String
    def stop(address: String, uuid: String): String
  }

  final case class PipelineRequest(name: String, rules: Map[String, String])
  implicit val PIPELINE_REQUEST_JSON_FORMAT: RootJsonFormat[PipelineRequest] = jsonFormat2(PipelineRequest)

  final case class ComponentAbstract(uuid: String, name: String, kind: String)
  val UNKNOWN_COMPONENT = ComponentAbstract(UNKNOWN, "unknown", "unknown")
  implicit val COMPONENT_ABSTRACT_JSON_FORMAT: RootJsonFormat[ComponentAbstract] = jsonFormat3(ComponentAbstract)

  final case class Pipeline(uuid: String,
                            name: String,
                            status: Status,
                            rules: Map[String, String],
                            objects: Seq[ComponentAbstract],
                            lastModified: Long)
      extends Data {
    override def kind: String = "pipeline"

    /**
      * @return true if all values have specified uuid. otherwise, false
      */
    def ready(): Boolean = rules.values.forall(_ != UNKNOWN)
  }
  implicit val PIPELINE_JSON_FORMAT: RootJsonFormat[Pipeline] = jsonFormat6(Pipeline)
  implicit val PIPELINE_COMMAND_FORMAT: DataCommandFormat[Pipeline] =
    new DataCommandFormat[Pipeline] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$PIPELINE_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$PIPELINE_PATH/$uuid"
    }
  implicit val PIPELINE_CONTROL_FORMAT: ControlCommandFormat[Pipeline] =
    new ControlCommandFormat[Pipeline] {
      override def start(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$PIPELINE_PATH/$uuid/start"
      override def stop(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$PIPELINE_PATH/$uuid/stop"
    }
  //------------------------------------------------[DATA-SOURCE]------------------------------------------------//
  val SOURCE_PATH = "sources"
  final case class SourceRequest(name: String, className: String, schema: Seq[Column], configs: Map[String, String])
  implicit val SOURCE_REQUEST_JSON_FORMAT: RootJsonFormat[SourceRequest] = new RootJsonFormat[SourceRequest] {
    override def write(obj: SourceRequest): JsValue = JsObject(
      "name" -> JsString(obj.name),
      "class" -> JsString(obj.className),
      "schema" -> JsArray(obj.schema.map(COLUMN_JSON_FORMAT.write).toVector),
      "configs" -> JsObject(obj.configs.map { case (k, v) => (k, JsString(v)) })
    )
    override def read(json: JsValue): SourceRequest =
      json.asJsObject.getFields("name", "class", "schema", "configs") match {
        case Seq(JsString(m), JsString(clz), JsArray(cs), JsObject(c)) =>
          SourceRequest(m, clz, cs.map(COLUMN_JSON_FORMAT.read), c.map {
            case (k, v) => (k, v.asInstanceOf[JsString].value)
          })
        // null schema is valid in SOURCE
        case Seq(JsString(m), JsString(clz), JsNull, JsObject(c)) =>
          SourceRequest(m, clz, Seq.empty, c.map { case (k, v) => (k, v.asInstanceOf[JsString].value) })
        case _ => throw new UnsupportedOperationException(s"invalid format of ${SourceRequest.getClass.getSimpleName}")
      }
  }

  final case class Source(uuid: String,
                          name: String,
                          className: String,
                          schema: Seq[Column],
                          configs: Map[String, String],
                          lastModified: Long)
      extends Data {
    override def kind: String = "source"
  }
  implicit val SOURCE_JSON_FORMAT: RootJsonFormat[Source] = new RootJsonFormat[Source] {
    override def write(obj: Source): JsValue = JsObject(
      "uuid" -> JsString(obj.uuid),
      "name" -> JsString(obj.name),
      "class" -> JsString(obj.className),
      "schema" -> JsArray(obj.schema.map(COLUMN_JSON_FORMAT.write).toVector),
      "configs" -> JsObject(obj.configs.map { case (k, v) => (k, JsString(v)) }),
      "lastModified" -> JsNumber(obj.lastModified)
    )
    override def read(json: JsValue): Source =
      json.asJsObject.getFields("uuid", "name", "class", "schema", "configs", "lastModified") match {
        case Seq(JsString(u), JsString(m), JsString(clz), JsArray(cs), JsObject(c), JsNumber(t)) =>
          Source(u, m, clz, cs.map(COLUMN_JSON_FORMAT.read), c.map {
            case (k, v) => (k, v.asInstanceOf[JsString].value)
          }, t.toLong)
        case _ => throw new UnsupportedOperationException(s"invalid format of ${Source.getClass.getSimpleName}")
      }
  }
  implicit val SOURCE_COMMAND_FORMAT: DataCommandFormat[Source] =
    new DataCommandFormat[Source] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$SOURCE_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$SOURCE_PATH/$uuid"
    }
  //------------------------------------------------[DATA-SINK]------------------------------------------------//
  val SINK_PATH = "sinks"
  final case class SinkRequest(name: String, className: String, schema: Seq[Column], configs: Map[String, String])
  implicit val SINK_REQUEST_JSON_FORMAT: RootJsonFormat[SinkRequest] = new RootJsonFormat[SinkRequest] {
    override def write(obj: SinkRequest): JsValue = JsObject(
      "name" -> JsString(obj.name),
      "class" -> JsString(obj.className),
      "schema" -> JsArray(obj.schema.map(COLUMN_JSON_FORMAT.write).toVector),
      "configs" -> JsObject(obj.configs.map { case (k, v) => (k, JsString(v)) })
    )
    override def read(json: JsValue): SinkRequest =
      json.asJsObject.getFields("name", "class", "schema", "configs") match {
        case Seq(JsString(m), JsString(clz), JsArray(cs), JsObject(c)) =>
          SinkRequest(m, clz, cs.map(COLUMN_JSON_FORMAT.read), c.map {
            case (k, v) => (k, v.asInstanceOf[JsString].value)
          })
        // null schema is valid in SOURCE
        case Seq(JsString(m), JsString(clz), JsNull, JsObject(c)) =>
          SinkRequest(m, clz, Seq.empty, c.map { case (k, v) => (k, v.asInstanceOf[JsString].value) })
        case _ => throw new UnsupportedOperationException(s"invalid format of ${SinkRequest.getClass.getSimpleName}")
      }
  }

  final case class Sink(uuid: String,
                        name: String,
                        className: String,
                        schema: Seq[Column],
                        configs: Map[String, String],
                        lastModified: Long)
      extends Data {
    override def kind: String = "sink"
  }
  implicit val SINK_JSON_FORMAT: RootJsonFormat[Sink] = new RootJsonFormat[Sink] {
    override def write(obj: Sink): JsValue = JsObject(
      "uuid" -> JsString(obj.uuid),
      "name" -> JsString(obj.name),
      "class" -> JsString(obj.className),
      "schema" -> JsArray(obj.schema.map(COLUMN_JSON_FORMAT.write).toVector),
      "configs" -> JsObject(obj.configs.map { case (k, v) => (k, JsString(v)) }),
      "lastModified" -> JsNumber(obj.lastModified)
    )
    override def read(json: JsValue): Sink =
      json.asJsObject.getFields("uuid", "name", "class", "schema", "configs", "lastModified") match {
        case Seq(JsString(u), JsString(m), JsString(clz), JsArray(cs), JsObject(c), JsNumber(t)) =>
          Sink(u,
               m,
               clz,
               cs.map(COLUMN_JSON_FORMAT.read),
               c.map { case (k, v) => (k, v.asInstanceOf[JsString].value) },
               t.toLong)
        case _ => throw new UnsupportedOperationException(s"invalid format of ${Sink.getClass.getSimpleName}")
      }
  }
  implicit val SINK_COMMAND_FORMAT: DataCommandFormat[Sink] =
    new DataCommandFormat[Sink] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$SINK_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$SINK_PATH/$uuid"
    }
  //------------------------------------------------[VALIDATION]------------------------------------------------//
  val VALIDATION_PATH = "validate"

  /**
    * used to send validation command
    */
  sealed trait ValidationCommandFormat[T] {
    def format(address: String): String
  }

  val HDFS_VALIDATION_PATH = "hdfs"
  final case class HdfsValidationRequest(uri: String)
  implicit val HDFS_VALIDATION_REQUEST_JSON_FORMAT: RootJsonFormat[HdfsValidationRequest] = jsonFormat1(
    HdfsValidationRequest)
  implicit val HDFS_VALIDATION_REQUEST_COMMAND_FORMAT: ValidationCommandFormat[HdfsValidationRequest] =
    new ValidationCommandFormat[HdfsValidationRequest] {
      override def format(address: String): String =
        s"http://$address/$VERSION_V0/$VALIDATION_PATH/$HDFS_VALIDATION_PATH"
    }

  val RDB_VALIDATION_PATH = "rdb"
  final case class RdbValidationRequest(uri: String, user: String, password: String)
  implicit val RDB_VALIDATION_REQUEST_JSON_FORMAT: RootJsonFormat[RdbValidationRequest] = jsonFormat3(
    RdbValidationRequest)
  implicit val RDB_VALIDATION_REQUEST_COMMAND_FORMAT: ValidationCommandFormat[RdbValidationRequest] =
    new ValidationCommandFormat[RdbValidationRequest] {
      override def format(address: String): String =
        s"http://$address/$VERSION_V0/$VALIDATION_PATH/$RDB_VALIDATION_PATH"
    }

  val FTP_VALIDATION_PATH = "ftp"
  final case class FtpValidationRequest(host: String, port: Int, user: String, password: String)
  implicit val FTP_VALIDATION_REQUEST_JSON_FORMAT: RootJsonFormat[FtpValidationRequest] =
    new RootJsonFormat[FtpValidationRequest] {
      override def read(json: JsValue): FtpValidationRequest =
        json.asJsObject.getFields("host", "port", "user", "password") match {
          case Seq(JsString(host), JsNumber(port), JsString(user), JsString(password)) =>
            FtpValidationRequest(host, port.toInt, user, password)
          // we will convert a Map[String, String] to FtpValidationRequest in kafka connector so this method can save us from spray's ClassCastException
          case Seq(JsString(host), JsString(port), JsString(user), JsString(password)) =>
            FtpValidationRequest(host, port.toInt, user, password)
          case _ =>
            throw new UnsupportedOperationException(s"invalid format of ${classOf[FtpValidationRequest].getSimpleName}")
        }

      override def write(obj: FtpValidationRequest): JsValue = JsObject(
        "host" -> JsString(obj.host),
        "port" -> JsNumber(obj.port),
        "user" -> JsString(obj.user),
        "password" -> JsString(obj.password)
      )
    }

  implicit val FTP_VALIDATION_REQUEST_COMMAND_FORMAT: ValidationCommandFormat[FtpValidationRequest] =
    new ValidationCommandFormat[FtpValidationRequest] {
      override def format(address: String): String =
        s"http://$address/$VERSION_V0/$VALIDATION_PATH/$FTP_VALIDATION_PATH"
    }

  final case class ValidationReport(hostname: String, message: String, pass: Boolean)
  implicit val VALIDATION_REPORT_JSON_FORMAT: RootJsonFormat[ValidationReport] = jsonFormat3(ValidationReport)

  //------------------------------------------------[RDB-QUERY]------------------------------------------------//
  val QUERY_PATH = "query"
  val RDB_PATH = "rdb"

  /**
    * used to query 3 party system
    */
  sealed trait QueryCommandFormat[T] {
    def format(address: String): String
  }

  final case class RdbColumn(name: String, typeName: String, pk: Boolean)
  implicit val RDB_COLUMN_JSON_FORMAT: RootJsonFormat[RdbColumn] = new RootJsonFormat[RdbColumn] {
    override def read(json: JsValue): RdbColumn = json.asJsObject.getFields("name", "type", "pk") match {
      case Seq(JsString(n), JsString(t), JsBoolean(pk)) => RdbColumn(n, t, pk)
      case _                                            => throw new UnsupportedOperationException(s"invalid format of ${RdbColumn.getClass.getSimpleName}")
    }
    override def write(obj: RdbColumn): JsValue = JsObject(
      "name" -> JsString(obj.name),
      "type" -> JsString(obj.typeName),
      "pk" -> JsBoolean(obj.pk)
    )
  }
  final case class RdbTable(catalogPattern: Option[String],
                            schemaPattern: Option[String],
                            name: String,
                            schema: Seq[RdbColumn])
  implicit val RDB_TABLE_JSON_FORMAT: RootJsonFormat[RdbTable] = jsonFormat4(RdbTable)

  final case class RdbQuery(url: String,
                            user: String,
                            password: String,
                            catalogPattern: Option[String],
                            schemaPattern: Option[String],
                            tableName: Option[String])
  implicit val RDB_QUERY_JSON_FORMAT: RootJsonFormat[RdbQuery] = jsonFormat6(RdbQuery)
  implicit val RDB_QUERY_COMMAND_FORMAT: QueryCommandFormat[RdbQuery] = new QueryCommandFormat[RdbQuery] {
    override def format(address: String): String = s"http://$address/$VERSION_V0/$QUERY_PATH/$RDB_PATH"
  }

  final case class RdbInformation(name: String, tables: Seq[RdbTable])
  implicit val RDB_INFORMATION_JSON_FORMAT: RootJsonFormat[RdbInformation] = jsonFormat2(RdbInformation)

  //------------------------------------------------[CLUSTER]------------------------------------------------//
  val CLUSTER_PATH = "cluster"

  /**
    * used to send cluster command
    */
  sealed trait ClusterCommandFormat[T] {
    def format(address: String): String
  }

  final case class ClusterInformation(brokers: String,
                                      workers: String,
                                      supportedDatabases: Seq[String],
                                      supportedDataTypes: Seq[DataType])
  implicit val CLUSTER_INFORMATION_JSON_FORMAT: RootJsonFormat[ClusterInformation] = jsonFormat4(ClusterInformation)
  implicit val CLUSTER_INFORMATION_COMMAND_FORMAT: ClusterCommandFormat[ClusterInformation] =
    new ClusterCommandFormat[ClusterInformation] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$CLUSTER_PATH"
    }
  //------------------------------------------------[ERROR]------------------------------------------------//
  final case class Error(code: String, message: String, stack: String)
  implicit val ERROR_JSON_FORMAT: RootJsonFormat[Error] = new RootJsonFormat[Error] {
    override def read(json: JsValue): Error = json.asJsObject.getFields("code", "message", "stack") match {
      case Seq(JsString(c), JsString(m), JsString(s)) => Error(c, m, s)
      case _                                          => throw new UnsupportedOperationException(s"invalid format of ${classOf[Error].getSimpleName}")
    }
    override def write(obj: Error): JsValue = JsObject(
      "code" -> JsString(obj.code),
      "message" -> JsString(obj.message),
      "stack" -> JsString(obj.stack)
    )
  }
  object Error {
    def apply(e: Throwable): Error =
      Error(e.getClass.getName, if (e.getMessage == null) "None" else e.getMessage, ExceptionUtils.getStackTrace(e))
  }
}
