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

package com.island.ohara.client.kafka

import java.util
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Collections, Objects, Properties}

import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.util.{CommonUtils, Releasable}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewPartitions, NewTopic, TopicDescription}
import org.apache.kafka.common.config.TopicConfig

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

/**
  * this is a wrap of kafka's AdminClient. However, we only wrap the functions about "topic" since the others are useless
  * to us.
  */
trait TopicAdmin extends Releasable {

  /**
    * Change the number of partitions of topic.
    * Currently, reducing the number of partitions is not allowed!
    * @param name topic name
    * @param numberOfPartitions the partitions that given topic should have
    * @return topic information
    */
  def changePartitions(name: String, numberOfPartitions: Int): Future[TopicAdmin.TopicInfo]

  /**
    * list all topics
    * @return topics information
    */
  def topics(): Future[Seq[TopicAdmin.TopicInfo]]

  /**
    * start a process to create topic
    * @return topic creator
    */
  def creator: TopicAdmin.Creator

  /**
    * delete a existent topic
    * @param name topic name
    * @return true if it does delete a topic. otherwise, false
    */
  def delete(name: String): Future[Boolean]

  /**
    * the connection information to kafka's broker
    * @return connection props
    */
  def connectionProps: String

  def closed: Boolean
}

object TopicAdmin {

  def apply(_connectionProps: String): TopicAdmin = new TopicAdmin {
    private[this] val _closed = new AtomicBoolean(false)
    override val connectionProps: String = _connectionProps

    override def closed: Boolean = _closed.get()

    /**
      * extract the exception wrapped in ExecutionException.
      * @param f action
      * @tparam T return type
      * @return return value
      */
    private[this] def unwrap[T](f: () => T): T = try f()
    catch {
      case e: ExecutionException =>
        throw e.getCause
    }
    private[this] def toAdminProps(connectionProps: String): Properties = {
      val adminProps = new Properties
      adminProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(connectionProps))
      adminProps
    }

    private[this] val admin = AdminClient.create(toAdminProps(connectionProps))

    override def close(): Unit = if (_closed.compareAndSet(false, true)) Releasable.close(admin)

    override def creator: Creator =
      (name: String, numberOfPartitions: Int, numberOfReplications: Short, cleanupPolicy: CleanupPolicy) => {
        val promise: Promise[TopicInfo] = Promise[TopicInfo]
        unwrap(
          () =>
            admin
              .createTopics(
                util.Collections.singletonList(new NewTopic(name, numberOfPartitions, numberOfReplications).configs(Map(
                  TopicConfig.CLEANUP_POLICY_CONFIG -> cleanupPolicy.name
                ).asJava)))
              .values()
              .get(name)
              .whenComplete((_, exception) => {
                if (exception == null)
                  promise.success(
                    TopicInfo(
                      name = name,
                      numberOfPartitions = numberOfPartitions,
                      numberOfReplications = numberOfReplications
                    ))
                else promise.failure(exception)
              }))
        promise.future
      }

    private[this] def toTopicInfo(desc: TopicDescription): TopicInfo = TopicInfo(
      name = desc.name(),
      numberOfPartitions = desc.partitions().size(),
      numberOfReplications = desc.partitions().get(0).replicas().size().asInstanceOf[Short]
    )

    override def topics(): Future[Seq[TopicInfo]] = {
      val promise = Promise[Seq[TopicInfo]]
      unwrap(
        () =>
          admin
            .listTopics()
            .names()
            .whenComplete((topicNames, exception) => {
              if (exception == null) {
                admin
                  .describeTopics(topicNames)
                  .all()
                  .whenComplete((topics, exception) => {
                    if (exception == null) promise.success(topics.values().asScala.map(toTopicInfo).toSeq)
                    else promise.failure(exception)
                  })
              } else promise.failure(exception)
            }))
      promise.future
    }

    override def changePartitions(name: String, numberOfPartitions: Int): Future[TopicInfo] = {
      val promise = Promise[TopicInfo]
      unwrap(
        () =>
          admin
            .describeTopics(util.Collections.singletonList(name))
            .all()
            .whenComplete((topics, exception) => {
              if (exception == null) {
                val topicOption = topics.values().asScala.find(_.name() == name)
                if (topicOption.isDefined) {
                  val topicInfo = toTopicInfo(topicOption.get)
                  if (topicInfo.numberOfPartitions > numberOfPartitions)
                    promise.failure(new IllegalArgumentException("Reducing the number from partitions is illegal"))
                  else if (topicInfo.numberOfPartitions == numberOfPartitions) promise.success(topicInfo)
                  else
                    unwrap(() =>
                      admin
                        .createPartitions(Collections.singletonMap(name, NewPartitions.increaseTo(numberOfPartitions)))
                        .all()
                        .whenComplete((_, exception) => {
                          if (exception == null)
                            promise.success(topicInfo.copy(numberOfPartitions = numberOfPartitions))
                          else promise.failure(exception)
                        }))
                } else promise.failure(new NoSuchElementException(s"$name doesn't exist"))
              } else promise.failure(exception)
            }))
      promise.future
    }

    override def delete(name: String): Future[Boolean] = {
      val promise = Promise[Boolean]
      unwrap(
        () =>
          admin
            .listTopics()
            .names()
            .whenComplete((topicNames, exception) => {
              if (exception == null) {
                if (topicNames.asScala.contains(name))
                  admin
                    .deleteTopics(util.Collections.singletonList(name))
                    .all()
                    .whenComplete((_, exception) => {
                      if (exception == null) promise.success(true)
                      else promise.failure(exception)
                    })
                else
                  promise.success(false)
              } else promise.failure(exception)
            }))
      promise.future
    }
  }

  final case class TopicInfo(name: String, numberOfPartitions: Int, numberOfReplications: Short)

  trait Creator extends com.island.ohara.common.pattern.Creator[Future[TopicAdmin.TopicInfo]] {
    private[this] var name: String = _
    private[this] var numberOfPartitions: Int = 1
    private[this] var numberOfReplications: Short = 1
    private[this] var cleanupPolicy: CleanupPolicy = CleanupPolicy.DELETE

    def name(name: String): Creator.this.type = {
      this.name = Objects.requireNonNull(name)
      this
    }

    @Optional("default is 1")
    def numberOfPartitions(numberOfPartitions: Int): Creator = {
      this.numberOfPartitions = CommonUtils.requirePositiveInt(numberOfPartitions)
      this
    }

    @Optional("default is 1")
    def numberOfReplications(numberOfReplications: Short): Creator = {
      this.numberOfReplications = CommonUtils.requirePositiveShort(numberOfReplications)
      this
    }

    @Optional("default is CleanupPolicy.DELETE")
    def cleanupPolicy(cleanupPolicy: CleanupPolicy): Creator = {
      this.cleanupPolicy = Objects.requireNonNull(cleanupPolicy)
      this
    }

    override def create(): Future[TopicAdmin.TopicInfo] = doCreate(
      name = Objects.requireNonNull(name),
      numberOfPartitions = CommonUtils.requirePositiveInt(numberOfPartitions),
      numberOfReplications = CommonUtils.requirePositiveShort(numberOfReplications),
      cleanupPolicy = Objects.requireNonNull(cleanupPolicy)
    )

    protected def doCreate(name: String,
                           numberOfPartitions: Int,
                           numberOfReplications: Short,
                           cleanupPolicy: CleanupPolicy): Future[TopicAdmin.TopicInfo]
  }
}
