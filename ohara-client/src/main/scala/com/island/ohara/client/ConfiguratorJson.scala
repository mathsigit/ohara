package com.island.ohara.client

import akka.http.scaladsl.model.Multipart.FormData.BodyPart.Strict
import com.island.ohara.client.ConnectorJson._
import com.island.ohara.common.data.connector.State
import com.island.ohara.common.data.{Column, DataType}
import org.apache.commons.lang3.exception.ExceptionUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{JsNull, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

/**
  * a collection from marshalling/unmarshalling configurator data to/from json.
  * NOTED: the json format is a part from PUBLIC INTERFACE so please don't change the field names after releasing the ohara.
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

  val START_COMMAND: String = "start"
  val STOP_COMMAND: String = "stop"
  val PAUSE_COMMAND: String = "pause"
  val RESUME_COMMAND: String = "resume"
  //------------------------------------------------[DATA]------------------------------------------------//
  sealed trait Data {
    def id: String
    def name: String

    /**
      * This field should not be marshalled into json so we make it be "def" rather than "val.
      * DON'T change this filed since it is exposed by restful APIs
      *
      * @return the type from this class
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

  implicit val COLUMN_JSON_FORMAT: RootJsonFormat[Column] = new RootJsonFormat[Column] {
    override def read(json: JsValue): Column = json.asJsObject.getFields("name", "newName", "dataType", "order") match {
      case Seq(JsString(n), JsString(nn), JsString(t), JsNumber(o)) => Column.of(n, nn, DataType.of(t), o.toInt)
      case Seq(JsString(n), JsNull, JsString(t), JsNumber(o))       => Column.of(n, n, DataType.of(t), o.toInt)
      case Seq(JsString(n), JsString(t), JsNumber(o))               => Column.of(n, n, DataType.of(t), o.toInt)
      case _                                                        => throw new UnsupportedOperationException(s"invalid format from ${classOf[Column].getSimpleName}")
    }
    override def write(obj: Column): JsValue = JsObject(
      "name" -> JsString(obj.name),
      "newName" -> JsString(obj.newName),
      "dataType" -> JsString(obj.dataType.name),
      "order" -> JsNumber(obj.order)
    )
  }
  //------------------------------------------------[DATA-TOPIC]------------------------------------------------//
  val TOPIC_INFO_PATH = "topics"
  final case class TopicInfoRequest(name: String, numberOfPartitions: Int, numberOfReplications: Short)
  implicit val TOPIC_INFO_REQUEST_JSON_FORMAT: RootJsonFormat[TopicInfoRequest] = jsonFormat3(TopicInfoRequest)

  final case class TopicInfo(id: String,
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

  final case class HdfsInformation(id: String, name: String, uri: String, lastModified: Long) extends Data {
    override def kind: String = "hdfs"
  }
  implicit val HDFS_INFORMATION_JSON_FORMAT: RootJsonFormat[HdfsInformation] = jsonFormat4(HdfsInformation)
  implicit val HDFS_INFORMATION_COMMAND_FORMAT: DataCommandFormat[HdfsInformation] =
    new DataCommandFormat[HdfsInformation] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$HDFS_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$HDFS_PATH/$uuid"
    }

  //------------------------------------------------[DATA-FTP]------------------------------------------------//
  val FTP_PATH = "ftp"
  final case class FtpInformationRequest(name: String, hostname: String, port: Int, user: String, password: String)
  implicit val FTP_INFORMATION_REQUEST_JSON_FORMAT: RootJsonFormat[FtpInformationRequest] = jsonFormat5(
    FtpInformationRequest)

  final case class FtpInformation(id: String,
                                  name: String,
                                  hostname: String,
                                  port: Int,
                                  user: String,
                                  password: String,
                                  lastModified: Long)
      extends Data {
    override def kind: String = "ftp"
  }
  implicit val FTP_INFORMATION_JSON_FORMAT: RootJsonFormat[FtpInformation] = jsonFormat7(FtpInformation)
  implicit val FTP_INFORMATION_COMMAND_FORMAT: DataCommandFormat[FtpInformation] =
    new DataCommandFormat[FtpInformation] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$FTP_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$FTP_PATH/$uuid"
    }

  //------------------------------------------------[DATA-JDBC]------------------------------------------------//
  val JDBC_PATH = "jdbc"
  final case class JdbcInformationRequest(name: String, url: String, user: String, password: String)
  implicit val JDBC_INFORMATION_REQUEST_JSON_FORMAT: RootJsonFormat[JdbcInformationRequest] = jsonFormat4(
    JdbcInformationRequest)

  final case class JdbcInformation(id: String,
                                   name: String,
                                   url: String,
                                   user: String,
                                   password: String,
                                   lastModified: Long)
      extends Data {
    override def kind: String = "jdbc"
  }
  implicit val JDBC_INFORMATION_JSON_FORMAT: RootJsonFormat[JdbcInformation] = jsonFormat6(JdbcInformation)
  implicit val JDBC_INFORMATION_COMMAND_FORMAT: DataCommandFormat[JdbcInformation] =
    new DataCommandFormat[JdbcInformation] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$JDBC_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$JDBC_PATH/$uuid"
    }

  //------------------------------------------------[DATA-PIPELINE]------------------------------------------------//
  val PIPELINE_PATH = "pipelines"

  /**
    * used to control data
    */
  sealed trait ControlCommandFormat[T] {

    /**
      * used to generate uri to send start request
      *
      * @param address basic address
      * @param uuid uuid from data
      * @return uri
      */
    def start(address: String, uuid: String): String

    /**
      * used to generate uri to send stop request
      *
      * @param address basic address
      * @param uuid uuid from data
      * @return uri
      */
    def stop(address: String, uuid: String): String

    /**
      * used to generate uri to send resume request
      *
      * @param address basic address
      * @param uuid uuid from data
      * @return uri
      */
    def resume(address: String, uuid: String): String

    /**
      * used to generate uri to send pause request
      *
      * @param address basic address
      * @param uuid uuid from data
      * @return uri
      */
    def pause(address: String, uuid: String): String
  }

  final case class PipelineRequest(name: String, rules: Map[String, String])
  implicit val PIPELINE_REQUEST_JSON_FORMAT: RootJsonFormat[PipelineRequest] = jsonFormat2(PipelineRequest)

  final case class ObjectAbstract(id: String, name: String, kind: String, state: Option[State]) extends Data
  implicit val OBJECT_ABSTRACT_JSON_FORMAT: RootJsonFormat[ObjectAbstract] = jsonFormat4(ObjectAbstract)

  final case class Pipeline(id: String,
                            name: String,
                            rules: Map[String, String],
                            objects: Seq[ObjectAbstract],
                            lastModified: Long)
      extends Data {
    override def kind: String = "pipeline"
  }
  implicit val PIPELINE_JSON_FORMAT: RootJsonFormat[Pipeline] = jsonFormat5(Pipeline)
  implicit val PIPELINE_COMMAND_FORMAT: DataCommandFormat[Pipeline] =
    new DataCommandFormat[Pipeline] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$PIPELINE_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$PIPELINE_PATH/$uuid"
    }
  //------------------------------------------------[DATA-SOURCE]------------------------------------------------//
  val SOURCE_PATH = "sources"
  final case class SourceRequest(name: String,
                                 className: String,
                                 schema: Seq[Column],
                                 topics: Seq[String],
                                 numberOfTasks: Int,
                                 configs: Map[String, String])
  implicit val SOURCE_REQUEST_JSON_FORMAT: RootJsonFormat[SourceRequest] = jsonFormat6(SourceRequest)

  final case class Source(id: String,
                          name: String,
                          className: String,
                          schema: Seq[Column],
                          topics: Seq[String],
                          numberOfTasks: Int,
                          configs: Map[String, String],
                          state: Option[State],
                          lastModified: Long)
      extends Data {
    override def kind: String = className
  }
  implicit val SOURCE_JSON_FORMAT: RootJsonFormat[Source] = jsonFormat9(Source)
  implicit val SOURCE_COMMAND_FORMAT: DataCommandFormat[Source] =
    new DataCommandFormat[Source] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$SOURCE_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$SOURCE_PATH/$uuid"
    }

  implicit val SOURCE_CONTROL_FORMAT: ControlCommandFormat[Source] =
    new ControlCommandFormat[Source] {
      override def start(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$SOURCE_PATH/$uuid/$START_COMMAND"
      override def stop(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$SOURCE_PATH/$uuid/$STOP_COMMAND"
      override def resume(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$SOURCE_PATH/$uuid/$RESUME_COMMAND"
      override def pause(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$SOURCE_PATH/$uuid/$PAUSE_COMMAND"
    }
  //------------------------------------------------[DATA-SINK]------------------------------------------------//
  val SINK_PATH = "sinks"
  final case class SinkRequest(name: String,
                               className: String,
                               schema: Seq[Column],
                               topics: Seq[String],
                               numberOfTasks: Int,
                               configs: Map[String, String])
  implicit val SINK_REQUEST_JSON_FORMAT: RootJsonFormat[SinkRequest] = jsonFormat6(SinkRequest)

  final case class Sink(id: String,
                        name: String,
                        className: String,
                        schema: Seq[Column],
                        topics: Seq[String],
                        numberOfTasks: Int,
                        configs: Map[String, String],
                        state: Option[State],
                        lastModified: Long)
      extends Data {
    override def kind: String = className
  }
  implicit val SINK_JSON_FORMAT: RootJsonFormat[Sink] = jsonFormat9(Sink)
  implicit val SINK_COMMAND_FORMAT: DataCommandFormat[Sink] =
    new DataCommandFormat[Sink] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$SINK_PATH"
      override def format(address: String, uuid: String): String = s"http://$address/$VERSION_V0/$SINK_PATH/$uuid"
    }

  implicit val SINK_CONTROL_FORMAT: ControlCommandFormat[Sink] =
    new ControlCommandFormat[Sink] {
      override def start(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$SINK_PATH/$uuid/$START_COMMAND"
      override def stop(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$SINK_PATH/$uuid/$STOP_COMMAND"
      override def resume(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$SINK_PATH/$uuid/$RESUME_COMMAND"
      override def pause(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$SINK_PATH/$uuid/$PAUSE_COMMAND"
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
  final case class RdbValidationRequest(url: String, user: String, password: String)
  implicit val RDB_VALIDATION_REQUEST_JSON_FORMAT: RootJsonFormat[RdbValidationRequest] = jsonFormat3(
    RdbValidationRequest)
  implicit val RDB_VALIDATION_REQUEST_COMMAND_FORMAT: ValidationCommandFormat[RdbValidationRequest] =
    new ValidationCommandFormat[RdbValidationRequest] {
      override def format(address: String): String =
        s"http://$address/$VERSION_V0/$VALIDATION_PATH/$RDB_VALIDATION_PATH"
    }

  val FTP_VALIDATION_PATH = "ftp"
  final case class FtpValidationRequest(hostname: String, port: Int, user: String, password: String)
  implicit val FTP_VALIDATION_REQUEST_JSON_FORMAT: RootJsonFormat[FtpValidationRequest] =
    new RootJsonFormat[FtpValidationRequest] {
      override def read(json: JsValue): FtpValidationRequest =
        json.asJsObject.getFields("hostname", "port", "user", "password") match {
          case Seq(JsString(hostname), JsNumber(port), JsString(user), JsString(password)) =>
            FtpValidationRequest(hostname, port.toInt, user, password)
          // we will convert a Map[String, String] to FtpValidationRequest in kafka connector so this method can save us from spray's ClassCastException
          case Seq(JsString(hostname), JsString(port), JsString(user), JsString(password)) =>
            FtpValidationRequest(hostname, port.toInt, user, password)
          case _ =>
            throw new UnsupportedOperationException(
              s"invalid format from ${classOf[FtpValidationRequest].getSimpleName}")
        }

      override def write(obj: FtpValidationRequest): JsValue = JsObject(
        "hostname" -> JsString(obj.hostname),
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

  final case class RdbColumn(name: String, dataType: String, pk: Boolean)
  implicit val RDB_COLUMN_JSON_FORMAT: RootJsonFormat[RdbColumn] = jsonFormat3(RdbColumn)
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
  final case class VersionInformation(version: String, user: String, revision: String, date: String)
  implicit val VERSION_JSON_FORMAT: RootJsonFormat[VersionInformation] = jsonFormat4(VersionInformation)

  val CLUSTER_PATH = "cluster"

  /**
    * used to send cluster command
    */
  sealed trait ClusterCommandFormat[T] {
    def format(address: String): String
  }
  final case class ConnectorInfo(className: String, version: String, revision: String)
  implicit val ConnectorInfo_JSON_FORMAT: RootJsonFormat[ConnectorInfo] = jsonFormat3(ConnectorInfo)

  final case class ClusterInformation(brokers: String,
                                      workers: String,
                                      sources: Seq[ConnectorInfo],
                                      sinks: Seq[ConnectorInfo],
                                      supportedDatabases: Seq[String],
                                      supportedDataTypes: Seq[DataType],
                                      versionInfo: VersionInformation)
  implicit val CLUSTER_INFORMATION_JSON_FORMAT: RootJsonFormat[ClusterInformation] = jsonFormat7(ClusterInformation)
  implicit val CLUSTER_INFORMATION_COMMAND_FORMAT: ClusterCommandFormat[ClusterInformation] =
    new ClusterCommandFormat[ClusterInformation] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$CLUSTER_PATH"
    }

  //------------------------------------------------[STREAM]------------------------------------------------//
  val STREAM_PATH = "stream"

  // Store object
  final case class StreamData(pipeline_id: String,
                              jarName: String,
                              name: String,
                              fromTopics: Seq[String],
                              toTopics: Seq[String],
                              instances: Int,
                              id: String,
                              filePath: String,
                              lastModified: Long)
      extends Data {
    override def kind: String = "streamApp"
  }

  // StreamApp List Request Body
  final case class StreamListRequest(jarName: String)
  implicit val STREAM_LIST_REQUEST_JSON_FORMAT: RootJsonFormat[StreamListRequest] = jsonFormat1(StreamListRequest)

  // StreamApp Property Request Body
  final case class StreamPropertyRequest(name: String, fromTopics: Seq[String], toTopics: Seq[String], instances: Int)
  implicit val STREAM_PROPERTY_REQUEST_JSON_FORMAT: RootJsonFormat[StreamPropertyRequest] = jsonFormat4(
    StreamPropertyRequest)

  // StreamApp List Page Response Body
  final case class StreamJar(id: String, jarName: String, lastModified: Long)
  implicit val STREAM_LIST_JARS_FORMAT: RootJsonFormat[StreamJar] = jsonFormat3(StreamJar)
  final case class StreamListResponse(jars: Seq[StreamJar])
  implicit val STREAM_LIST_RESPONSE_JSON_FORMAT: RootJsonFormat[StreamListResponse] = jsonFormat1(StreamListResponse)

  // StreamApp Property Page Response Body
  final case class StreamPropertyResponse(id: String,
                                          jarName: String,
                                          name: String,
                                          fromTopics: Seq[String],
                                          toTopics: Seq[String],
                                          instances: Int,
                                          lastModified: Long)
  implicit val STREAM_PROPERTY_RESPONSE_JSON_FORMAT: RootJsonFormat[StreamPropertyResponse] = jsonFormat7(
    StreamPropertyResponse)

  // StreamApp List Page Command
  val JARS_STREAM_PATH = "jars"
  implicit val JAR_UPLOAD_LIST_PATH_COMMAND_FORMAT: DataCommandFormat[Strict] =
    new DataCommandFormat[Strict] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$STREAM_PATH/$JARS_STREAM_PATH"
      override def format(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$STREAM_PATH/$JARS_STREAM_PATH/$uuid"
    }
  implicit val LIST_PATH_COMMAND_FORMAT: DataCommandFormat[StreamJar] =
    new DataCommandFormat[StreamJar] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$STREAM_PATH/$JARS_STREAM_PATH"
      override def format(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$STREAM_PATH/$JARS_STREAM_PATH/$uuid"
    }

  // StreamApp Property Page
  val PROPERTY_STREAM_PATH = "property"
  implicit val PROPERTY_INFO_COMMAND_FORMAT: DataCommandFormat[StreamPropertyResponse] =
    new DataCommandFormat[StreamPropertyResponse] {
      override def format(address: String): String = s"http://$address/$VERSION_V0/$STREAM_PATH/$PROPERTY_STREAM_PATH"
      override def format(address: String, uuid: String): String =
        s"http://$address/$VERSION_V0/$STREAM_PATH/$PROPERTY_STREAM_PATH/$uuid"
    }

  //------------------------------------------------[ERROR]------------------------------------------------//
  final case class Error(code: String, message: String, stack: String)
  implicit val ERROR_JSON_FORMAT: RootJsonFormat[Error] = new RootJsonFormat[Error] {
    override def read(json: JsValue): Error = json.asJsObject.getFields("code", "message", "stack") match {
      case Seq(JsString(c), JsString(m), JsString(s)) => Error(c, m, s)
      case _                                          => throw new UnsupportedOperationException(s"invalid format from ${classOf[Error].getSimpleName}")
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
