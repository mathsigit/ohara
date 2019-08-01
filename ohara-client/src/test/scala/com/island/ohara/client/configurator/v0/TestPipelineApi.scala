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

package com.island.ohara.client.configurator.v0

import com.island.ohara.client.configurator.v0.PipelineApi._
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.kafka.connector.json.ObjectKey
import org.junit.Test
import org.scalatest.Matchers
import spray.json.DeserializationException

import scala.concurrent.ExecutionContext.Implicits.global
import spray.json._
class TestPipelineApi extends SmallTest with Matchers {

  @Test
  def nullKeyInGet(): Unit =
    an[NullPointerException] should be thrownBy PipelineApi.access.get(null)

  @Test
  def nullKeyInDelete(): Unit =
    an[NullPointerException] should be thrownBy PipelineApi.access.delete(null)

  @Test
  def parseDeprecatedJsonOfPipelineCreationRequest(): Unit = {
    val from = CommonUtils.randomString()
    val to0 = CommonUtils.randomString()
    val to1 = CommonUtils.randomString()
    val req = PIPELINE_CREATION_JSON_FORMAT.read(s"""
                                               |{
                                               |  "name":"${CommonUtils.randomString()}",
                                               |  "rules": {
                                               |    "$from": [
                                               |      "$to0", "$to1"
                                               |    ]
                                               |  }
                                               |}
                                            """.stripMargin.parseJson)
    req.flows.size shouldBe 1
    req.flows.head.from shouldBe ObjectKey.of(Data.GROUP_DEFAULT, from)
    req.flows.head.to.size shouldBe 2
    req.flows.head.to shouldBe Set(ObjectKey.of(Data.GROUP_DEFAULT, to0), ObjectKey.of(Data.GROUP_DEFAULT, to1))
  }
  @Test
  def parseDeprecatedJsonOfPipeline(): Unit = {
    val pipeline = Pipeline(
      group = CommonUtils.randomString(),
      name = CommonUtils.randomString(),
      workerClusterName = Some(CommonUtils.randomString()),
      objects = Set.empty,
      flows = Seq.empty,
      lastModified = CommonUtils.current(),
      tags = Map.empty
    )
    val json = PIPELINE_JSON_FORMAT.write(pipeline).toString
    withClue(json)(json.contains("\"rules\":{") shouldBe true)
  }

  @Test
  def parseDeprecatedJsonOfPipeline2(): Unit = {
    val from = ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString())
    val to = ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString())
    val pipeline = Pipeline(
      group = CommonUtils.randomString(),
      name = CommonUtils.randomString(),
      workerClusterName = Some(CommonUtils.randomString()),
      objects = Set.empty,
      flows = Seq(
        Flow(
          from = from,
          to = Set(to)
        )
      ),
      lastModified = CommonUtils.current(),
      tags = Map.empty
    )
    val json = PIPELINE_JSON_FORMAT.write(pipeline).toString
    withClue(json)(json.contains(s"""\"rules\":{\"${from.name}\":[\"${to.name}\"]""") shouldBe true)
  }

  @Test
  def ignoreNameOnCreation(): Unit = PipelineApi.access
    .hostname(CommonUtils.randomString())
    .port(CommonUtils.availablePort())
    .request
    .creation
    .name
    .length should not be 0

  @Test
  def ignoreNameOnUpdate(): Unit = an[NullPointerException] should be thrownBy PipelineApi.access
    .hostname(CommonUtils.randomString())
    .port(CommonUtils.availablePort())
    .request
    .update()

  @Test
  def emptyGroup(): Unit = an[IllegalArgumentException] should be thrownBy PipelineApi.access.request.group("")

  @Test
  def nullGroup(): Unit = an[NullPointerException] should be thrownBy PipelineApi.access.request.group(null)

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy PipelineApi.access.request.name("")

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy PipelineApi.access.request.name(null)

  @Test
  def emptyWorkerClusterName(): Unit =
    an[IllegalArgumentException] should be thrownBy PipelineApi.access.request.workerClusterName("")

  @Test
  def nullWorkerClusterName(): Unit =
    an[NullPointerException] should be thrownBy PipelineApi.access.request.workerClusterName(null)

  @Test
  def emptyFlows(): Unit = {
    // pass since the update request requires the empty list
    PipelineApi.access.request.flows(Seq.empty)
  }

  @Test
  def nullFlows(): Unit = an[NullPointerException] should be thrownBy PipelineApi.access.request.flows(null)

  @Test
  def parseFlow(): Unit = {
    val from = ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString())
    val to = ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString())
    val flow = FLOW_JSON_FORMAT.read(s"""
        |  {
        |    "from": {
        |      "group": "${from.group}",
        |      "name": "${from.name}"
        |    },
        |    "to": [
        |      {
        |        "group": "${to.group}",
        |        "name": "${to.name}"
        |      }
        |    ]
        |  }
        |
    """.stripMargin.parseJson)
    flow.from shouldBe from
    flow.to.size shouldBe 1
    flow.to.head shouldBe to
  }

  @Test
  def emptyFromInFlow(): Unit = an[DeserializationException] should be thrownBy FLOW_JSON_FORMAT.read("""
      |  {
      |    "from": "",
      |    "to": ["to"]
      |  }
      |
    """.stripMargin.parseJson)

  @Test
  def emptyToInFlow(): Unit = an[DeserializationException] should be thrownBy FLOW_JSON_FORMAT.read("""
      |  {
      |    "from": "aaa",
      |    "to": [""]
      |  }
      |
    """.stripMargin.parseJson)

  @Test
  def parseCreation(): Unit = {
    val creation = PIPELINE_CREATION_JSON_FORMAT.read(s"""
                                                          |  {
                                                          |  }
                                                          |
    """.stripMargin.parseJson)
    creation.group shouldBe Data.GROUP_DEFAULT
    creation.name.length shouldBe 10
    creation.workerClusterName shouldBe None
    creation.flows shouldBe Seq.empty

    val group = CommonUtils.randomString()
    val name = CommonUtils.randomString()
    val creation2 = PIPELINE_CREATION_JSON_FORMAT.read(s"""
        |  {
        |    "group": "$group",
        |    "name": "$name"
        |  }
        |
    """.stripMargin.parseJson)
    creation2.group shouldBe group
    creation2.name shouldBe name
    creation2.workerClusterName shouldBe None
    creation2.flows shouldBe Seq.empty
    creation2.tags shouldBe Map.empty
  }

  @Test
  def emptyNameInCreation(): Unit =
    an[DeserializationException] should be thrownBy PIPELINE_CREATION_JSON_FORMAT.read("""
      |  {
      |    "name": ""
      |  }
      |
    """.stripMargin.parseJson)
  @Test
  def emptyWorkerClusterNameInCreation(): Unit =
    an[DeserializationException] should be thrownBy PIPELINE_CREATION_JSON_FORMAT.read("""
      |  {
      |    "name": "aaa",
      |    "workerClusterName": ""
      |  }
      |
    """.stripMargin.parseJson)

  @Test
  def emptyWorkerClusterNameInUpdate(): Unit =
    an[DeserializationException] should be thrownBy PIPELINE_UPDATE_JSON_FORMAT.read("""
      |  {
      |    "workerClusterName": ""
      |  }
      |
    """.stripMargin.parseJson)

  @Test
  def nullTags(): Unit = an[NullPointerException] should be thrownBy PipelineApi.access.request.tags(null)

  @Test
  def emptyTags(): Unit = PipelineApi.access.request.tags(Map.empty)
}
