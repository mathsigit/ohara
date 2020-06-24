/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oharastream.ohara.client.configurator

import java.util.Objects

import oharastream.ohara.client.configurator.QueryRequest
import oharastream.ohara.client.configurator.ClusterAccess.Query
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.setting.SettingDef.{Reference, Type}
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.{CommonUtils, VersionUtils}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.concurrent.{ExecutionContext, Future}
object WorkerApi {
  val KIND: String = SettingDef.Reference.WORKER.name().toLowerCase

  @deprecated(message = s"replaced by $KIND", since = "0.11.0")
  val WORKER_PREFIX_PATH: String = "workers"

  /**
    * the default docker image used to run containers of worker cluster
    */
  val IMAGE_NAME_DEFAULT: String = s"oharastream/connect-worker:${VersionUtils.VERSION}"
  val BROKER_CLUSTER_KEY_KEY     = "brokerClusterKey"
  val PLUGIN_KEYS_KEY            = "pluginKeys"

  val SHARED_JAR_KEYS_KEY = "sharedJarKeys"
  val FREE_PORTS_KEY      = "freePorts"
  val GROUP_ID_KEY        = "group.id"

  val STATUS_TOPIC_NAME_KEY         = "status.storage.topic"
  val STATUS_TOPIC_PARTITIONS_KEY   = "status.storage.partitions"
  val STATUS_TOPIC_REPLICATIONS_KEY = "status.storage.replication.factor"

  val CONFIG_TOPIC_NAME_KEY         = "config.storage.topic"
  val CONFIG_TOPIC_PARTITIONS_KEY   = "config.storage.partitions"
  val CONFIG_TOPIC_REPLICATIONS_KEY = "config.storage.replication.factor"

  val OFFSET_TOPIC_NAME_KEY         = "offset.storage.topic"
  val OFFSET_TOPIC_PARTITIONS_KEY   = "offset.storage.partitions"
  val OFFSET_TOPIC_REPLICATIONS_KEY = "offset.storage.replication.factor"

  val DEFINITIONS: Seq[SettingDef] = DefinitionCollector()
    .addFollowupTo("core")
    .group()
    .name()
    .imageName(IMAGE_NAME_DEFAULT)
    .nodeNames()
    .routes()
    .tags()
    .definition(
      _.key(BROKER_CLUSTER_KEY_KEY)
        .documentation("broker cluster used to store data for this worker cluster")
        .required(Type.OBJECT_KEY)
        .reference(Reference.BROKER)
    )
    .definition(
      _.key(GROUP_ID_KEY)
        .documentation("group ID of this worker cluster")
        .stringWithRandomDefault()
    )
    .addFollowupTo("performance")
    .definition(
      _.key(STATUS_TOPIC_NAME_KEY)
        .documentation("name of status topic which is used to store connector status")
        .stringWithRandomDefault("connect.status")
    )
    .definition(
      _.key(STATUS_TOPIC_PARTITIONS_KEY)
        .documentation("number of partitions for status topic")
        .positiveNumber(1)
    )
    .definition(
      _.key(STATUS_TOPIC_REPLICATIONS_KEY)
        .documentation("number of replications for status topic")
        .positiveNumber(1.asInstanceOf[Short])
    )
    .definition(
      _.key(CONFIG_TOPIC_NAME_KEY)
        .documentation("name of config topic which is used to store connector config")
        .stringWithRandomDefault("connect.config")
    )
    .definition(
      _.key(CONFIG_TOPIC_PARTITIONS_KEY)
        .documentation("number of partitions for config topic. this value MUST be 1")
        .positiveNumber(1)
        .permission(SettingDef.Permission.READ_ONLY)
    )
    .definition(
      _.key(CONFIG_TOPIC_REPLICATIONS_KEY)
        .documentation("number of replications for config topic")
        .positiveNumber(1.asInstanceOf[Short])
    )
    .definition(
      _.key(OFFSET_TOPIC_NAME_KEY)
        .documentation("name of offset topic which is used to store connector data offset")
        .stringWithRandomDefault("connect.offset")
    )
    .definition(
      _.key(OFFSET_TOPIC_PARTITIONS_KEY)
        .documentation("number of partitions for offset topic")
        .positiveNumber(1)
    )
    .definition(
      _.key(OFFSET_TOPIC_REPLICATIONS_KEY)
        .documentation("number of replications for offset topic")
        .positiveNumber(1.asInstanceOf[Short])
    )
    .initHeap()
    .maxHeap()
    .addFollowupTo("public")
    .definition(
      _.key(PLUGIN_KEYS_KEY)
        .documentation("the files containing your connectors")
        .optional(Type.OBJECT_KEYS)
        .reference(Reference.FILE)
    )
    .definition(
      _.key(SHARED_JAR_KEYS_KEY)
        .documentation("the shared jars")
        .optional(Type.OBJECT_KEYS)
        .reference(Reference.FILE)
    )
    .definition(
      _.key(FREE_PORTS_KEY)
        .documentation("the pre-binding ports for this worker cluster.")
        .optional(Type.ARRAY)
    )
    .clientPort()
    .jmxPort()
    .result

  final class Creation private[WorkerApi] (val raw: Map[String, JsValue]) extends ClusterCreation {
    /**
      * reuse the parser from Update.
      *
      * @return update
      */
    private[this] implicit def update(raw: Map[String, JsValue]): Updating = new Updating(noJsNull(raw))
    def brokerClusterKey: ObjectKey                                        = raw.brokerClusterKey.get
    def clientPort: Int                                                    = raw.clientPort.get
    def groupId: String                                                    = raw.groupId.get
    def statusTopicName: String                                            = raw.statusTopicName.get
    def statusTopicPartitions: Int                                         = raw.statusTopicPartitions.get
    def statusTopicReplications: Short                                     = raw.statusTopicReplications.get
    def configTopicName: String                                            = raw.configTopicName.get
    def configTopicReplications: Short                                     = raw.configTopicReplications.get
    def offsetTopicName: String                                            = raw.offsetTopicName.get
    def offsetTopicPartitions: Int                                         = raw.offsetTopicPartitions.get
    def offsetTopicReplications: Short                                     = raw.offsetTopicReplications.get
    def pluginKeys: Set[ObjectKey]                                         = raw.pluginKeys.getOrElse(Set.empty)
    def sharedJarKeys: Set[ObjectKey]                                      = raw.sharedJarKeys.getOrElse(Set.empty)
    def freePorts: Set[Int]                                                = raw.freePorts.get

    override def ports: Set[Int] = freePorts + clientPort + jmxPort

    // TODO: we should allow connector developers to define volume and then use it
    // https://github.com/oharastream/ohara/issues/4621
    override def volumeMaps: Map[ObjectKey, String] = Map.empty
  }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val CREATION_FORMAT: JsonRefiner[Creation] =
    rulesOfCreation[Creation](
      new RootJsonFormat[Creation] {
        override def write(obj: Creation): JsValue = JsObject(noJsNull(obj.raw))
        override def read(json: JsValue): Creation = new Creation(json.asJsObject.fields)
      },
      DEFINITIONS
    )

  final class Updating private[WorkerApi] (val raw: Map[String, JsValue]) extends ClusterUpdating {
    def brokerClusterKey: Option[ObjectKey] = noJsNull(raw).get(BROKER_CLUSTER_KEY_KEY).map(_.convertTo[ObjectKey])
    def clientPort: Option[Int]             = noJsNull(raw).get(CLIENT_PORT_KEY).map(_.convertTo[Int])
    def groupId: Option[String]             = noJsNull(raw).get(GROUP_ID_KEY).map(_.convertTo[String])
    def statusTopicName: Option[String]     = noJsNull(raw).get(STATUS_TOPIC_NAME_KEY).map(_.convertTo[String])
    def statusTopicPartitions: Option[Int]  = noJsNull(raw).get(STATUS_TOPIC_PARTITIONS_KEY).map(_.convertTo[Int])
    def statusTopicReplications: Option[Short] =
      noJsNull(raw).get(STATUS_TOPIC_REPLICATIONS_KEY).map(_.convertTo[Short])
    def configTopicName: Option[String] = noJsNull(raw).get(CONFIG_TOPIC_NAME_KEY).map(_.convertTo[String])
    def configTopicReplications: Option[Short] =
      noJsNull(raw).get(CONFIG_TOPIC_REPLICATIONS_KEY).map(_.convertTo[Short])
    def offsetTopicName: Option[String]    = noJsNull(raw).get(OFFSET_TOPIC_NAME_KEY).map(_.convertTo[String])
    def offsetTopicPartitions: Option[Int] = noJsNull(raw).get(OFFSET_TOPIC_PARTITIONS_KEY).map(_.convertTo[Int])
    def offsetTopicReplications: Option[Short] =
      noJsNull(raw).get(OFFSET_TOPIC_REPLICATIONS_KEY).map(_.convertTo[Short])
    def pluginKeys: Option[Set[ObjectKey]] = noJsNull(raw).get(PLUGIN_KEYS_KEY).map(_.convertTo[Set[ObjectKey]])
    def sharedJarKeys: Option[Set[ObjectKey]] =
      noJsNull(raw).get(SHARED_JAR_KEYS_KEY).map(_.convertTo[Set[ObjectKey]])
    def freePorts: Option[Set[Int]] =
      noJsNull(raw).get(FREE_PORTS_KEY).map(_.convertTo[Set[Int]])
  }
  implicit val UPDATING_FORMAT: JsonRefiner[Updating] =
    rulesOfUpdating[Updating](
      new RootJsonFormat[Updating] {
        override def write(obj: Updating): JsValue = JsObject(noJsNull(obj.raw))
        override def read(json: JsValue): Updating = new Updating(json.asJsObject.fields)
      }
    )

  final case class WorkerClusterInfo private[ohara] (
    settings: Map[String, JsValue],
    aliveNodes: Set[String],
    override val lastModified: Long,
    state: Option[ClusterState],
    error: Option[String]
  ) extends ClusterInfo {
    /**
      * reuse the parser from Creation.
      *
      * @return creation
      */
    private[this] implicit def creation(raw: Map[String, JsValue]): Creation = new Creation(noJsNull(raw))
    def brokerClusterKey: ObjectKey                                          = settings.brokerClusterKey
    def clientPort: Int                                                      = settings.clientPort
    def groupId: String                                                      = settings.groupId
    def statusTopicName: String                                              = settings.statusTopicName
    def statusTopicPartitions: Int                                           = settings.statusTopicPartitions
    def statusTopicReplications: Short                                       = settings.statusTopicReplications
    def configTopicName: String                                              = settings.configTopicName
    def configTopicPartitions: Int                                           = 1
    def configTopicReplications: Short                                       = settings.configTopicReplications
    def offsetTopicName: String                                              = settings.offsetTopicName
    def offsetTopicPartitions: Int                                           = settings.offsetTopicPartitions
    def offsetTopicReplications: Short                                       = settings.offsetTopicReplications
    def pluginKeys: Set[ObjectKey]                                           = settings.pluginKeys
    def sharedJarKeys: Set[ObjectKey]                                        = settings.sharedJarKeys
    def freePorts: Set[Int]                                                  = settings.freePorts

    /**
      * the node names is not equal to "running" nodes. The connection props may reference to invalid nodes and the error
      * should be handled by the client code.
      * @return a string host_0:port,host_1:port
      */
    def connectionProps: String =
      if (nodeNames.isEmpty) throw new IllegalArgumentException("there is no nodes!!!")
      else nodeNames.map(n => s"$n:$clientPort").mkString(",")

    override def ports: Set[Int] = settings.ports

    override def kind: String = KIND

    override def raw: Map[String, JsValue] = WORKER_CLUSTER_INFO_FORMAT.write(this).asJsObject.fields
  }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val WORKER_CLUSTER_INFO_FORMAT: JsonRefiner[WorkerClusterInfo] =
    JsonRefinerBuilder[WorkerClusterInfo]
      .format(new RootJsonFormat[WorkerClusterInfo] {
        private[this] val format                            = jsonFormat5(WorkerClusterInfo)
        override def read(json: JsValue): WorkerClusterInfo = format.read(extractSetting(json.asJsObject))
        override def write(obj: WorkerClusterInfo): JsValue = flattenSettings(format.write(obj).asJsObject)
      })
      .build

  /**
    * used to generate the payload and url for POST/PUT request.
    * this request is extended by collie also so it is public than sealed.
    */
  trait Request extends ClusterRequest {
    @Optional("the default port is random")
    def clientPort(clientPort: Int): Request.this.type =
      setting(CLIENT_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(clientPort)))

    @Optional("the default port is random")
    def jmxPort(jmxPort: Int): Request.this.type =
      setting(JMX_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(jmxPort)))

    @Optional("Ignoring the name will invoke an auto-mapping to existent broker cluster")
    def brokerClusterKey(brokerClusterKey: ObjectKey): Request.this.type =
      setting(BROKER_CLUSTER_KEY_KEY, OBJECT_KEY_FORMAT.write(Objects.requireNonNull(brokerClusterKey)))

    @Optional("the default port is random")
    def groupId(groupId: String): Request.this.type =
      setting(GROUP_ID_KEY, JsString(CommonUtils.requireNonEmpty(groupId)))
    @Optional("the default port is random")
    def statusTopicName(statusTopicName: String): Request.this.type =
      setting(STATUS_TOPIC_NAME_KEY, JsString(CommonUtils.requireNonEmpty(statusTopicName)))
    @Optional("the default number is 1")
    def statusTopicPartitions(statusTopicPartitions: Int): Request.this.type =
      setting(STATUS_TOPIC_PARTITIONS_KEY, JsNumber(CommonUtils.requirePositiveInt(statusTopicPartitions)))
    @Optional("the default number is 1")
    def statusTopicReplications(statusTopicReplications: Short): Request.this.type =
      setting(STATUS_TOPIC_REPLICATIONS_KEY, JsNumber(CommonUtils.requirePositiveShort(statusTopicReplications)))
    @Optional("the default number is random")
    def configTopicName(configTopicName: String): Request.this.type =
      setting(CONFIG_TOPIC_NAME_KEY, JsString(CommonUtils.requireNonEmpty(configTopicName)))
    @Optional("the default number is 1")
    def configTopicReplications(configTopicReplications: Short): Request.this.type =
      setting(CONFIG_TOPIC_REPLICATIONS_KEY, JsNumber(CommonUtils.requirePositiveShort(configTopicReplications)))
    def offsetTopicName(offsetTopicName: String): Request.this.type =
      setting(OFFSET_TOPIC_NAME_KEY, JsString(CommonUtils.requireNonEmpty(offsetTopicName)))
    @Optional("the default number is 1")
    def offsetTopicPartitions(offsetTopicPartitions: Int): Request.this.type =
      setting(OFFSET_TOPIC_PARTITIONS_KEY, JsNumber(CommonUtils.requirePositiveInt(offsetTopicPartitions)))
    @Optional("the default number is 1")
    def offsetTopicReplications(offsetTopicReplications: Short): Request.this.type =
      setting(OFFSET_TOPIC_REPLICATIONS_KEY, JsNumber(CommonUtils.requirePositiveShort(offsetTopicReplications)))

    @Optional("the default value is empty")
    def pluginKeys(pluginKeys: Set[ObjectKey]): Request.this.type =
      setting(PLUGIN_KEYS_KEY, JsArray(pluginKeys.map(OBJECT_KEY_FORMAT.write).toVector))

    @Optional("the default value is empty")
    def sharedJarKeys(sharedJarKeys: Set[ObjectKey]): Request.this.type =
      setting(SHARED_JAR_KEYS_KEY, JsArray(sharedJarKeys.map(OBJECT_KEY_FORMAT.write).toVector))

    @Optional("default value is empty array in creation and None in update")
    def tags(tags: Map[String, JsValue]): Request.this.type = setting(TAGS_KEY, JsObject(tags))

    /**
      * set the port to pre-bind by this worker cluster
      * @param port port to pre-bind
      * @return this request
      */
    def freePort(port: Int): Request.this.type = freePorts(Set(port))
    def freePorts(ports: Set[Int]): Request.this.type =
      setting(FREE_PORTS_KEY, JsArray(ports.map(JsNumber(_)).toVector))

    /**
      * Creation instance includes many useful parsers for custom settings so we open it to code with a view to reusing
      * those convenient parsers.
      * @return the payload of creation
      */
    final def creation: Creation =
      CREATION_FORMAT.read(CREATION_FORMAT.write(new Creation(noJsNull(settings.toMap))))

    /**
      * for testing only
      * @return the payload of update
      */
    private[configurator] final def updating: Updating =
      UPDATING_FORMAT.read(UPDATING_FORMAT.write(new Updating(noJsNull(settings.toMap))))
  }

  /**
    * similar to Request but it has execution methods.
    *
    */
  sealed trait ExecutableRequest extends Request {
    def create()(implicit executionContext: ExecutionContext): Future[WorkerClusterInfo]
    def update()(implicit executionContext: ExecutionContext): Future[WorkerClusterInfo]
  }

  final class Access private[WorkerApi] extends ClusterAccess[Creation, Updating, WorkerClusterInfo](KIND) {
    override def query: Query[WorkerClusterInfo] = new Query[WorkerClusterInfo] {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext
      ): Future[Seq[WorkerClusterInfo]] = list(request)
    }

    def request: ExecutableRequest = new ExecutableRequest {
      override def create()(implicit executionContext: ExecutionContext): Future[WorkerClusterInfo] = post(creation)

      override def update()(implicit executionContext: ExecutionContext): Future[WorkerClusterInfo] =
        put(key, updating)
    }
  }

  def access: Access = new Access
}
