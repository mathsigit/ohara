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

import oharastream.ohara.common.data.{Cell, Row}
import oharastream.ohara.common.setting.SettingDef.Type
import oharastream.ohara.common.setting._
import oharastream.ohara.common.util.CommonUtils
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
package object v0 {
  /**
    * Our first version of APIs!!!
    */
  val V0: String = "v0"

  /**
    * the default group to all objects.
    * the group is useful to Ohara Manager. However, in simple case, the group is a bit noisy so we offer the default group to all objects when
    * input group is ignored.
    */
  val GROUP_DEFAULT: String = "default"
  val GROUP_KEY: String     = "group"

  /**
    * All services are able to bind a port to provide access.
    */
  val CLIENT_PORT_KEY = "clientPort"

  /**
    * All services are able to bind a port to provide metrics access.
    */
  val JMX_PORT_KEY = "jmxPort"

  /**
    * Noted: there are other two definition having "name"
    * 1) ConnectorDefUtils.CONNECTOR_NAME_DEFINITION
    * 2) StreamDefinitions.NAME_DEFINITION
    */
  val NAME_KEY: String = "name"

  val LAST_MODIFIED_KEY: String = "lastModified"

  /**
    * the extra routes to this service.
    */
  val ROUTES_KEY: String = "routes"

  /**
    * Noted: there are other two definition having "tags""
    * 1) ConnectorDefUtils.TAGS_DEFINITION
    * 2) StreamDefinitions.TAGS_DEFINITION
    */
  val TAGS_KEY: String = "tags"

  /**
    * equals to jvm's xms
    */
  val INIT_HEAP_KEY: String = "xms"

  /**
    * equals to jvm's xmx
    */
  val MAX_HEAP_KEY: String = "xmx"

  /**
    * the objects containing custom settings have this field.
    */
  val SETTINGS_KEY: String = "settings"

  /**
    * Noted: there are other two definition having "nodeNames""
    * 1) StreamDefinitions.NODE_NAMES_DEFINITION
    */
  val NODE_NAMES_KEY: String = "nodeNames"
  val IMAGE_NAME_KEY: String = "imageName"
  val FORCE_KEY: String      = "force"
  val START_COMMAND: String  = "start"
  val STOP_COMMAND: String   = "stop"
  val PAUSE_COMMAND: String  = "pause"
  val RESUME_COMMAND: String = "resume"

  /**
    * In this APIs we have to integrate json format between scala (spray-json) and java (jackson).
    * The JsNull generated by spray-json confuse jackson to generate many "null" object. We remove the key related to
    * JsNull in order to avoid passing null to jackson.
    */
  private[v0] def noJsNull(fields: Map[String, JsValue]): Map[String, JsValue] = fields.filter {
    _._2 match {
      case JsNull => false
      case _      => true
    }
  }

  private[v0] def noJsNull(jsValue: JsValue): Map[String, JsValue] = noJsNull(jsValue.asJsObject.fields)

  // accessable to ohara-configurator module
  private[ohara] implicit val OBJECT_KEY_FORMAT: RootJsonFormat[ObjectKey] = JsonRefinerBuilder[ObjectKey]
    .format(new RootJsonFormat[ObjectKey] {
      override def write(obj: ObjectKey): JsValue = ObjectKey.toJsonString(obj).parseJson

      private[this] def read(fields: Map[String, JsValue]): ObjectKey = {
        def string(key: String): Option[String] = noJsNull(fields).get(key).map {
          case JsString(s) if s.nonEmpty => s
          case _ =>
            throw DeserializationException(s"the $key in ObjectKey must be non-empty string", fieldNames = List(key))
        }
        ObjectKey.of(
          string(GROUP_KEY).getOrElse(GROUP_DEFAULT),
          string(NAME_KEY).getOrElse(
            throw DeserializationException(s"$NAME_KEY is required field", fieldNames = List(NAME_KEY))
          )
        )
      }

      override def read(json: JsValue): ObjectKey = json match {
        case JsString(s)      => read(Map(GROUP_KEY -> JsString(GROUP_DEFAULT), NAME_KEY -> JsString(s)))
        case JsObject(fields) => read(fields)
        case _ =>
          throw DeserializationException(
            "the form of key must be {\"group\": \"g\", \"name\": \"n\"}, {\"name\": \"n\"} or pure string"
          )
      }
    })
    .nullToString(GROUP_KEY, () => GROUP_DEFAULT)
    .rejectEmptyString()
    .build

  private[v0] implicit val TOPIC_KEY_FORMAT: RootJsonFormat[TopicKey] = new RootJsonFormat[TopicKey] {
    override def write(obj: TopicKey): JsValue = TopicKey.toJsonString(obj).parseJson
    override def read(json: JsValue): TopicKey = {
      // reuse the rules of ObjectKey
      val key = OBJECT_KEY_FORMAT.read(json)
      TopicKey.of(key.group(), key.name())
    }
  }

  private[v0] implicit val CONNECTOR_KEY_FORMAT: RootJsonFormat[ConnectorKey] = new RootJsonFormat[ConnectorKey] {
    override def write(obj: ConnectorKey): JsValue = ConnectorKey.toJsonString(obj).parseJson
    override def read(json: JsValue): ConnectorKey = {
      // reuse the rules of ObjectKey
      val key = OBJECT_KEY_FORMAT.read(json)
      ConnectorKey.of(key.group(), key.name())
    }
  }

  private[v0] implicit val PROP_GROUP_FORMAT: RootJsonFormat[PropGroup] = new RootJsonFormat[PropGroup] {
    override def write(obj: PropGroup): JsValue =
      JsArray(
        obj
          .raw()
          .asScala
          .map(_.asScala.map {
            case (key, value) => key -> JsString(value)
          }.toMap)
          .map(JsObject(_))
          .toVector
      )
    override def read(json: JsValue): PropGroup =
      try PropGroup.ofJson(json.toString())
      catch {
        case e: Throwable =>
          throw DeserializationException(s"failed to convert $json to PropGroup", e)
      }
  }

  /**
    * exposed to configurator
    */
  implicit val SETTING_DEFINITION_JSON_FORMAT: RootJsonFormat[SettingDef] =
    new RootJsonFormat[SettingDef] {
      override def read(json: JsValue): SettingDef = SettingDef.ofJson(json.toString())
      override def write(obj: SettingDef): JsValue = obj.toJsonString.parseJson
    }

  private[v0] implicit val DURATION_JSON_FORMAT: RootJsonFormat[Duration] =
    new RootJsonFormat[Duration] {
      override def read(json: JsValue): Duration = json match {
        case JsString(s) =>
          try CommonUtils.toDuration(s).toMillis milliseconds
          catch {
            case _: Throwable =>
              throw DeserializationException(s"the value must be duration value, actual:$s")
          }
        case _ => throw DeserializationException(s"must be string type, actual:${json.getClass.getName}")
      }

      override def write(obj: Duration): JsValue = JsString(obj.toString)
    }

  /**
    * use basic check rules of object key for json refiner:
    * <p> 1) name and group must satisfy the regex [a-z0-9]
    * <p> 2) name will use randomString if not defined.
    * <p> 3) group will use defaultGroup if not defined.
    * <p> 4) name length + group length <= LIMIT_OF_KEY_LENGTH
    *
    * @tparam T type of object
    * @return json refiner object
    */
  private[v0] def rulesOfKey[T]: JsonRefinerBuilder[T] =
    limitsOfKey[T]
    // we random a default name for this object
      .nullToString(NAME_KEY, () => CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT))
      .nullToString(GROUP_KEY, () => GROUP_DEFAULT)

  /**
    * add limits to group and name.
    * NOTED: this rules don't include the default value to group!!!
    * @return refiner
    */
  private[v0] def limitsOfKey[T]: JsonRefinerBuilder[T] =
    JsonRefinerBuilder[T]
      .stringRestriction(GROUP_KEY, SettingDef.GROUP_STRING_REGEX)
      .stringRestriction(NAME_KEY, SettingDef.NAME_STRING_REGEX)

  /**
    * use basic check rules of creation request for json refiner.
    * 1) reject any empty string.
    * 2) nodeName cannot use "start" and "stop" keywords.
    * 3) nodeName cannot be empty array.
    * 4) imageName will use {defaultImage} if not defined.
    * 5) tags will use empty map if not defined.
    * @tparam T type of creation
    * @return json refiner object
    */
  private[v0] def rulesOfCreation[T <: ClusterCreation](
    format: RootJsonFormat[T],
    definitions: Seq[SettingDef]
  ): JsonRefiner[T] =
    limitsOfKey[T]
      .format(format)
      .definitions(definitions)
      // for each field, we should reject any empty string
      .rejectEmptyString()
      .build

  /**
    * use basic check rules of update request for json refiner.
    * 1) reject any empty string.
    * 2) nodeName cannot use "start" and "stop" keywords.
    * 3) nodeName cannot be empty array.
    * @tparam T type of update
    * @return json refiner object
    */
  private[v0] def rulesOfUpdating[T <: ClusterUpdating](format: RootJsonFormat[T]): JsonRefiner[T] =
    JsonRefinerBuilder[T]
      .format(format)
      // for each field, we should reject any empty string
      .rejectEmptyString()
      //-------------------------------------- "nodeNames" rules ---------------------------------//
      .arrayRestriction(NODE_NAMES_KEY)
      // we use the same sub-path for "node" and "actions" urls:
      // xxx/cluster/{name}/{node}
      // xxx/cluster/{name}/[start|stop]
      // the "actions" keywords must be avoided in nodeNames parameter
      .rejectKeyword(START_COMMAND)
      .rejectKeyword(STOP_COMMAND)
      .toRefiner
      .build

  private[this] def toJson(value: Any): JsValue = value match {
    //--------[primitive type]--------//
    case b: Boolean     => JsBoolean(b)
    case s: String      => JsString(s)
    case i: Short       => JsNumber(i)
    case i: Int         => JsNumber(i)
    case i: Long        => JsNumber(i)
    case i: Float       => JsNumber(i)
    case i: Double      => JsNumber(i)
    case _: Array[Byte] => JsString("binary data")
    case b: Byte        => JsNumber(b)
    //--------[for scala]--------//
    case i: BigDecimal  => JsNumber(i)
    case s: Iterable[_] => JsArray(s.map(toJson).toVector)
    //--------[ohara data]--------//
    case c: Cell[_] => JsObject(c.name() -> toJson(c.value()))
    case r: Row     => toJson(r)
    //--------[for java]--------//
    case i: java.math.BigDecimal  => JsNumber(i)
    case s: java.lang.Iterable[_] => JsArray(s.asScala.map(toJson).toVector)
    //--------[other]--------//
    case _ => throw new IllegalArgumentException(s"${value.getClass.getName} is unsupported!!!")
  }

  /**
    * convert the row to json representation.
    * This is a common conversion in Ohara since it is the bridge between http and data in topic.
    * @param row row
    * @return json representation
    */
  def toJson(row: Row): JsObject = JsObject(
    row.cells().asScala.map(cell => cell.name() -> toJson(cell.value())).toMap + (TAGS_KEY -> JsArray(
      row.tags().asScala.map(JsString(_)).toVector
    ))
  )

  private[this] def toValue(value: JsValue): Any = value match {
    case JsNull       => throw new IllegalArgumentException("null should be eliminated")
    case JsBoolean(b) => b
    case JsNumber(i)  => i
    case JsString(s)  => s
    case JsArray(es) =>
      es.filter {
          case JsNull => false
          case _      => true
        }
        .map(toValue)
        .toList
    case obj: JsObject => toRow(obj)
  }

  /**
    * convert the json representation to row.
    * This is a common conversion in Ohara since it is the bridge between http and data in topic.
    * @param obj json represention
    * @return row
    */
  def toRow(obj: JsObject): Row = Row.of(
    noJsNull(obj.fields)
      .get(TAGS_KEY)
      .map {
        case s: JsArray => s
        case _          => throw DeserializationException(s"$TAGS_KEY must be array type", fieldNames = List(TAGS_KEY))
      }
      .map(_.elements.map(_.convertTo[String]))
      .getOrElse(Seq.empty)
      .asJava,
    noJsNull(obj.fields.filter(_._1 != TAGS_KEY)).map {
      case (name, value) =>
        Cell.of(name, toValue(value))
    }.toSeq: _*
  )

  //------------------[quick builder for cluster services]------------------//
  private[v0] def groupDefinition: SettingDef.Builder => SettingDef =
    _.key(GROUP_KEY)
      .documentation("group of this worker cluster")
      .optional(GROUP_DEFAULT)
      .permission(SettingDef.Permission.CREATE_ONLY)
      .build()

  private[v0] def nameDefinition: SettingDef.Builder => SettingDef =
    _.key(NAME_KEY)
      .documentation("name of this worker cluster")
      .stringWithRandomDefault()
      .permission(SettingDef.Permission.CREATE_ONLY)
      .build()

  private[v0] def imageNameDefinition(defaultImage: String): SettingDef.Builder => SettingDef =
    _.key(IMAGE_NAME_KEY)
      .optional(defaultImage)
      .documentation("the docker image of this service")
      .permission(SettingDef.Permission.CREATE_ONLY)
      .build()

  private[v0] def clientPortDefinition: SettingDef.Builder => SettingDef =
    _.key(CLIENT_PORT_KEY).documentation("the port used to expose the service").bindingPortWithRandomDefault().build()

  private[v0] def jmxPortDefinition: SettingDef.Builder => SettingDef =
    _.key(JMX_PORT_KEY)
      .documentation("the port used to expose the metrics of this cluster")
      .bindingPortWithRandomDefault()
      .build()

  private[v0] def nodeDefinition: SettingDef.Builder => SettingDef =
    _.key(NODE_NAMES_KEY)
      .documentation("the nodes hosting this cluster")
      .blacklist(Set(START_COMMAND, STOP_COMMAND, PAUSE_COMMAND, RESUME_COMMAND).asJava)
      .build()

  private[v0] def routesDefinition: SettingDef.Builder => SettingDef =
    _.key(ROUTES_KEY).documentation("the extra routes to this service").optional(Type.TAGS).build()

  private[v0] def tagsDefinition: SettingDef.Builder => SettingDef =
    _.key(TAGS_KEY).documentation("the tags to this cluster").optional(Type.TAGS).build()

  private[v0] def maxHeapDefinition: SettingDef.Builder => SettingDef =
    _.key(MAX_HEAP_KEY).documentation("maximum memory allocation (in MB)").positiveNumber(1024L).build()

  private[v0] def initHeapDefinition: SettingDef.Builder => SettingDef =
    _.key(INIT_HEAP_KEY).documentation("initial heap size (in MB)").positiveNumber(1024L).build()

  private[v0] def flattenSettings(obj: JsObject): JsObject =
    JsObject(
      noJsNull(
        obj.fields.get(SETTINGS_KEY).map(_.asJsObject.fields).getOrElse(Map.empty)
        // we override the key in settings if it is conflict to runtime key
          ++ obj.fields
          - SETTINGS_KEY
      )
    )

  private[this] val runtimeKeys = Set(
    "aliveNodes",
    "lastModified",
    "state",
    "error",
    "partitionInfos",
    "nodeMetrics",
    "metrics",
    "status",
    "tasksStatus",
    SETTINGS_KEY
  )

  /**
    * it removes all keys related to runtime information.
    * TODO: we hardcode the "runtime" key here and count on our tests to check all keys are added ...
    * @param obj json representation
    * @return filtered objs
    */
  private[v0] def extractSetting(obj: JsObject): JsObject =
    JsObject(noJsNull(obj.fields + (SETTINGS_KEY -> JsObject(obj.fields.filterNot(e => runtimeKeys.contains(e._1))))))
}
