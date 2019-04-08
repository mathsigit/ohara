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

package com.island.ohara.agent
import java.util.Objects

import com.island.ohara.agent.Collie.ClusterCreator
import com.island.ohara.client.configurator.v0.ClusterInfo
import com.island.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import com.island.ohara.common.util.CommonUtils

import scala.concurrent.{ExecutionContext, Future}

/**
  * Collie is a cute dog helping us to "manage" a bunch of sheep.
  * @tparam T cluster description
  */
trait Collie[T <: ClusterInfo, Creator <: ClusterCreator[T]] {

  /**
    * remove whole cluster by specified name.
    * NOTED: Graceful downing whole cluster may take some time...
    * @param clusterName cluster name
    */
  def remove(clusterName: String)(implicit executionContext: ExecutionContext): Future[T]

  /**
    * get logs from all containers.
    * NOTED: It is ok to get logs from a "dead" cluster.
    * @param clusterName cluster name
    * @return all log content from cluster. Each container has a log.
    */
  def logs(clusterName: String)(implicit executionContext: ExecutionContext): Future[Map[ContainerInfo, String]]

  /**
    * create a cluster creator
    * @return creator of broker cluster
    */
  def creator(): Creator

  /**
    * get the containers information from a1 cluster
    * @param clusterName cluster name
    * @return containers information
    */
  def containers(clusterName: String)(implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]] =
    cluster(clusterName).map(_._2)

  def clusters(implicit executionContext: ExecutionContext): Future[Map[T, Seq[ContainerInfo]]]

  /**
    * get the cluster information from a broker cluster
    * @param name cluster name
    * @return cluster information
    */
  def cluster(name: String)(implicit executionContext: ExecutionContext): Future[(T, Seq[ContainerInfo])] =
    clusters.map(_.find(_._1.name == name).getOrElse(throw new NoSuchClusterException(s"$name doesn't exist")))

  /**
    * @param clusterName cluster name
    * @return true if the broker cluster exists
    */
  def exist(clusterName: String)(implicit executionContext: ExecutionContext): Future[Boolean] =
    clusters.map(_.exists(_._1.name == clusterName))

  /**
    * @param clusterName cluster name
    * @return true if the broker cluster doesn't exist
    */
  def nonExist(clusterName: String)(implicit executionContext: ExecutionContext): Future[Boolean] =
    exist(clusterName).map(!_)

  /**
    * add a node to a running broker cluster
    * NOTED: this is a async operation since graceful adding a node to a running service may be slow.
    * @param clusterName cluster name
    * @param nodeName node name
    * @return updated broker cluster
    */
  def addNode(clusterName: String, nodeName: String)(implicit executionContext: ExecutionContext): Future[T]

  /**
    * remove a node from a running broker cluster.
    * NOTED: this is a async operation since graceful downing a node from a running service may be slow.
    * @param clusterName cluster name
    * @param nodeName node name
    * @return updated broker cluster
    */
  def removeNode(clusterName: String, nodeName: String)(implicit executionContext: ExecutionContext): Future[T]
}

object Collie {

  /**
    * docker does limit the length of name (< 64). And we "may" salt the name so we release only 30 chars to user.
    */
  private[agent] val LIMIT_OF_NAME_LENGTH: Int = 30

  private[this] def assertLength(s: String): String = if (s.length > LIMIT_OF_NAME_LENGTH)
    throw new IllegalArgumentException(s"limit of length is $LIMIT_OF_NAME_LENGTH. actual: ${s.length}")
  else s

  trait ClusterCreator[T <: ClusterInfo] {
    protected var imageName: String = _
    protected var clusterName: String = _
    protected var nodeNames: Seq[String] = _

    /**
      * set the image name used to create cluster's container.
      * Currently, there are three kind of services are supported by community. zk, bk and wk.
      * @param imageName image name
      * @return this creator
      */
    def imageName(imageName: String): ClusterCreator.this.type = {
      this.imageName = CommonUtils.requireNonEmpty(imageName)
      this
    }

    /**
      * set the cluster name. Noted: All collie impls are unsupported to use duplicate cluster name.
      * @param clusterName cluster name
      * @return this creator
      */
    def clusterName(clusterName: String): ClusterCreator.this.type = {
      this.clusterName = assertLength(CommonUtils.assertOnlyNumberAndChar(CommonUtils.requireNonEmpty(clusterName)))
      this
    }

    /**
      *  create a single-node cluster.
      *  NOTED: this is a async method since starting a cluster is always gradual.
      * @param nodeName node name
      * @return cluster description
      */
    def nodeName(nodeName: String): ClusterCreator.this.type = nodeNames(Seq(nodeName))

    /**
      *  create a cluster.
      *  NOTED: this is a async method since starting a cluster is always gradual.
      * @param nodeNames nodes' name
      * @return cluster description
      */
    def nodeNames(nodeNames: Seq[String]): ClusterCreator.this.type = {
      this.nodeNames = requireNonEmpty(nodeNames)
      this
    }

    /**
      * CommonUtils.requireNonEmpty can't serve for scala so we write this method...by chia
      * @param s input seq
      * @tparam E element type
      * @tparam T seq type
      * @return input seq
      */
    protected def requireNonEmpty[C <: Seq[_]](s: C): C = if (Objects.requireNonNull(s).isEmpty)
      throw new IllegalArgumentException("empty seq is illegal!!!")
    else s

    def create()(implicit executionContext: ExecutionContext): Future[T]
  }
}
