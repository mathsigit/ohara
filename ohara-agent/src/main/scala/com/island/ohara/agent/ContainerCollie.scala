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

import com.island.ohara.agent.Collie.ClusterCreator
import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import com.island.ohara.client.configurator.v0.ClusterInfo
import com.island.ohara.client.configurator.v0.ContainerApi.{ContainerInfo, PortMapping, PortPair}
import com.island.ohara.client.configurator.v0.StreamApi.StreamClusterInfo
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.client.configurator.v0.NodeApi.Node
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.{ClassTag, classTag}

abstract class ContainerCollie[T <: ClusterInfo: ClassTag, Creator <: ClusterCreator[T]](nodeCollie: NodeCollie)
    extends Collie[T, Creator] {
  private[agent] val LENGTH_OF_CONTAINER_NAME_ID: Int = 7

  /**
    * generate unique name for the container.
    * It can be used in setting container's hostname and name
    * @param clusterName cluster name
    * @return a formatted string. form: ${clusterName}-${service}-${index}
    */
  protected def format(prefixKey: String, clusterName: String, serviceName: String): String =
    Seq(
      prefixKey,
      clusterName,
      serviceName,
      CommonUtils.randomString(LENGTH_OF_CONTAINER_NAME_ID)
    ).mkString(ContainerCollie.DIVIDER)

  protected def doRemoveNode(previousCluster: T, beRemovedContainer: ContainerInfo)(
    implicit executionContext: ExecutionContext): Future[Boolean]

  override final def removeNode(clusterName: String, nodeName: String)(
    implicit executionContext: ExecutionContext): Future[Boolean] = clusters.flatMap(
    _.find(_._1.name == clusterName)
      .filter(_._1.nodeNames.contains(nodeName))
      .filter(_._2.exists(_.nodeName == nodeName))
      .map {
        case (cluster, runningContainers) =>
          runningContainers.size match {
            case 1 =>
              Future.failed(new IllegalArgumentException(
                s"$clusterName is a single-node cluster. You can't remove the last node by removeNode(). Please use remove(clusterName) instead"))
            case _ =>
              doRemoveNode(
                cluster,
                runningContainers
                  .find(_.nodeName == nodeName)
                  .getOrElse(throw new IllegalArgumentException(
                    s"This should not be happen!!! $nodeName doesn't exist on cluster:$clusterName"))
              )
          }
      }
      .getOrElse(Future.successful(false)))

  protected def doAddNode(previousCluster: T, previousContainers: Seq[ContainerInfo], newNodeName: String)(
    implicit executionContext: ExecutionContext): Future[T]

  override final def addNode(clusterName: String, nodeName: String)(
    implicit executionContext: ExecutionContext): Future[T] = {
    nodeCollie
      .node(nodeName) // make sure there is a exist node.
      .flatMap(_ => cluster(clusterName))
      .flatMap {
        case (cluster, containers) => {
          if (clusterName.isEmpty || nodeName.isEmpty)
            Future.failed(new IllegalArgumentException("cluster and node name can't empty"))
          else if (CommonUtils.hasUpperCase(nodeName))
            Future.failed(new IllegalArgumentException("Your node name can't uppercase"))
          else doAddNode(cluster, containers, nodeName)
        }
      }
  }
  protected def serviceName: String =
    if (classTag[T].runtimeClass.isAssignableFrom(classOf[ZookeeperClusterInfo])) ContainerCollie.ZK_SERVICE_NAME
    else if (classTag[T].runtimeClass.isAssignableFrom(classOf[BrokerClusterInfo])) ContainerCollie.BK_SERVICE_NAME
    else if (classTag[T].runtimeClass.isAssignableFrom(classOf[WorkerClusterInfo])) ContainerCollie.WK_SERVICE_NAME
    else if (classTag[T].runtimeClass.isAssignableFrom(classOf[StreamClusterInfo])) ContainerCollie.STREAM_SERVICE_NAME
    else throw new IllegalArgumentException(s"Who are you, ${classTag[T].runtimeClass} ???")

  override final def forceRemove(clusterName: String)(implicit executionContext: ExecutionContext): Future[Boolean] =
    clusters.flatMap(
      _.find(_._1.name == clusterName)
        .map {
          case (cluster, containerInfos) => doForceRemove(cluster, containerInfos)
        }
        .getOrElse(Future.successful(false)))

  override final def remove(clusterName: String)(implicit executionContext: ExecutionContext): Future[Boolean] =
    clusters.flatMap(
      _.find(_._1.name == clusterName)
        .map {
          case (cluster, containerInfos) => doRemove(cluster, containerInfos)
        }
        .getOrElse(Future.successful(false)))

  protected def doRemove(clusterInfo: T, containerInfos: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext): Future[Boolean]

  protected def doForceRemove(clusterInfo: T, containerInfos: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext): Future[Boolean] =
    doRemove(clusterInfo, containerInfos)

  /**
    * This is a complicated process. We must address following issues.
    * 1) check the existence of cluster
    * 2) check the existence of nodes
    * 3) Each zookeeper container has got to export peer port, election port, and client port
    * 4) Each zookeeper container should use "docker host name" to replace "container host name".
    * 4) Add routes to all zookeeper containers
    * @return creator of broker cluster
    */
  protected[agent] def zkCreator(
    prefixKey: String,
    clusterName: String,
    imageName: String,
    clientPort: Int,
    peerPort: Int,
    electionPort: Int,
    nodeNames: Seq[String])(executionContext: ExecutionContext): Future[ZookeeperClusterInfo] = {
    implicit val exec: ExecutionContext = executionContext
    clusters.flatMap(clusters => {
      if (clusters.keys.filter(_.isInstanceOf[ZookeeperClusterInfo]).exists(_.name == clusterName))
        Future.failed(new IllegalArgumentException(s"zookeeper cluster:$clusterName exists!"))
      else
        nodeCollie
          .nodes(nodeNames)
          .map(_.map(node => node -> format(prefixKey, clusterName, serviceName)).toMap)
          .flatMap {
            nodes =>
              // add route in order to make zk node can connect to each other.
              val route: Map[String, String] = routeInfo(nodes)

              val zkServers: String = nodes.keys.map(_.name).mkString(" ")
              // ssh connection is slow so we submit request by multi-thread
              Future
                .sequence(nodes.zipWithIndex.map {
                  case ((node, containerName), index) =>
                    Future {
                      val containerInfo = ContainerInfo(
                        nodeName = node.name,
                        id = ContainerCollie.UNKNOWN,
                        imageName = imageName,
                        created = ContainerCollie.UNKNOWN,
                        state = ContainerCollie.UNKNOWN,
                        kind = ContainerCollie.UNKNOWN,
                        name = containerName,
                        size = ContainerCollie.UNKNOWN,
                        portMappings = Seq(PortMapping(
                          hostIp = ContainerCollie.UNKNOWN,
                          portPairs = Seq(
                            PortPair(
                              hostPort = clientPort,
                              containerPort = clientPort
                            ),
                            PortPair(
                              hostPort = peerPort,
                              containerPort = peerPort
                            ),
                            PortPair(
                              hostPort = electionPort,
                              containerPort = electionPort
                            )
                          )
                        )),
                        environments = Map(
                          ZookeeperCollie.ID_KEY -> index.toString,
                          ZookeeperCollie.CLIENT_PORT_KEY -> clientPort.toString,
                          ZookeeperCollie.PEER_PORT_KEY -> peerPort.toString,
                          ZookeeperCollie.ELECTION_PORT_KEY -> electionPort.toString,
                          ZookeeperCollie.SERVERS_KEY -> zkServers
                        ),
                        // zookeeper doesn't have advertised hostname/port so we assign the "docker host" directly
                        hostname = node.name
                      )
                      doCreator(executionContext, clusterName, containerName, containerInfo, node, route)
                      Some(containerInfo)
                    }
                })
                .map(_.flatten.toSeq)
                .map {
                  successfulContainers =>
                    if (successfulContainers.isEmpty)
                      throw new IllegalArgumentException(s"failed to create $clusterName on $serviceName")
                    val clusterInfo = ZookeeperClusterInfo(
                      name = clusterName,
                      imageName = imageName,
                      clientPort = clientPort,
                      peerPort = peerPort,
                      electionPort = electionPort,
                      nodeNames = successfulContainers.map(_.nodeName)
                    )
                    postCreateZookeeperCluster(clusterInfo, successfulContainers)
                    clusterInfo
                }
          }
    })

  }

  protected def doCreator(executionContext: ExecutionContext,
                          clusterName: String,
                          containerName: String,
                          containerInfo: ContainerInfo,
                          node: Node,
                          route: Map[String, String]): Unit

  protected def postCreateZookeeperCluster(clusterInfo: ClusterInfo, successfulContainers: Seq[ContainerInfo]): Unit = {
    //Default Nothing
  }

  protected def routeInfo(nodes: Map[Node, String]): Map[String, String] =
    nodes.map {
      case (node, _) =>
        node.name -> CommonUtils.address(node.name)
    }

}

object ContainerCollie {
  val ZK_SERVICE_NAME: String = "zk"
  val BK_SERVICE_NAME: String = "bk"
  val WK_SERVICE_NAME: String = "wk"

  /**
    * container name is controlled by streamRoute, the service name here use five words was ok.
    */
  val STREAM_SERVICE_NAME: String = "stream"

  /**
    * used to distinguish the cluster name and service name
    */
  val DIVIDER: String = "-"
  val UNKNOWN: String = "unknown"
}