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

package com.island.ohara.agent.docker

import java.util.Objects

import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.util.CommonUtils

/**
  * A interface used to run a docker container on remote node
  */
trait ContainerCreator {
  private[this] var hostname: String = CommonUtils.randomString()
  private[this] var imageName: String = _
  private[this] var name: String = CommonUtils.randomString()
  private[this] var command: String = ""
  private[this] var _removeContainerOnExit: Boolean = false
  private[this] var ports: Map[Int, Int] = Map.empty
  private[this] var envs: Map[String, String] = Map.empty
  private[this] var route: Map[String, String] = Map.empty
  private[this] var volumeMapping: Map[String, String] = Map.empty
  private[this] var networkDriver: NetworkDriver = NetworkDriver.BRIDGE

  /**
    * execute the docker container on background.
    */
  def execute(): Unit = doExecute(
    hostname = CommonUtils.requireNonEmpty(hostname),
    imageName = CommonUtils.requireNonEmpty(imageName),
    name = CommonUtils.requireNonEmpty(name),
    command = command,
    removeContainerOnExit = _removeContainerOnExit,
    ports = ports,
    envs = envs,
    route = route,
    volumeMapping = volumeMapping,
    networkDriver = networkDriver
  )
  protected def doExecute(hostname: String,
                          imageName: String,
                          name: String,
                          command: String,
                          removeContainerOnExit: Boolean,
                          ports: Map[Int, Int],
                          envs: Map[String, String],
                          route: Map[String, String],
                          volumeMapping: Map[String, String],
                          networkDriver: NetworkDriver): Unit

  /**
    * set container's name. default is a random string
    *
    * @param name container name
    * @return this builder
    */
  @Optional("default is random string")
  def name(name: String): ContainerCreator = {
    this.name = CommonUtils.requireNonEmpty(name)
    this
  }

  /**
    * set target image
    *
    * @param imageName docker image
    * @return this builder
    */
  def imageName(imageName: String): ContainerCreator = {
    this.imageName = CommonUtils.requireNonEmpty(imageName)
    this
  }

  /**
    * @param hostname the hostname of container
    * @return this builder
    */
  @Optional("default is random string")
  def hostname(hostname: String): ContainerCreator = {
    this.hostname = CommonUtils.requireNonEmpty(hostname)
    this
  }

  /**
    * @param envs the env variables exposed to container
    * @return this builder
    */
  @Optional("default is empty")
  def envs(envs: Map[String, String]): ContainerCreator = {
    this.envs = assertNonEmpty(envs)
    this
  }

  /**
    * @param route the pre-defined route to container. hostname -> ip
    * @return this builder
    */
  @Optional("default is empty")
  def route(route: Map[String, String]): ContainerCreator = {
    this.route = assertNonEmpty(route)
    this
  }

  /**
    * forward the port from host to container.
    * NOTED: currently we don't support to specify the network interface so the forwarded port is bound on all network adapters.
    *
    * @param ports port mapping (host's port -> container's port)
    * @return this builder
    */
  @Optional("default is empty")
  def portMappings(ports: Map[Int, Int]): ContainerCreator = {
    this.ports = assertNonEmpty(ports)
    this
  }

  /**
    * docker -v
    *
    * @return process information
    */
  @Optional("default is empty")
  def volumeMapping(volumeMapping: Map[String, String]): ContainerCreator = {
    this.volumeMapping = assertNonEmpty(volumeMapping)
    this
  }

  /**
    * set docker container's network driver. implement by --network=$value
    *
    * @param driver network driver
    * @return this builder
    */
  @Optional("default is NetworkDriver.BRIDGE")
  def networkDriver(networkDriver: NetworkDriver): ContainerCreator = {
    this.networkDriver = Objects.requireNonNull(networkDriver)
    this
  }

  /**
    * set true if you want to clean up the dead container automatically
    *
    * @return executor
    */
  @Optional("default is false")
  def removeContainerOnExit(): ContainerCreator = {
    this._removeContainerOnExit = true
    this
  }

  /**
    * the command passed to docker container
    *
    * @param command command
    * @return this builder
    */
  @Optional("default is empty")
  def command(command: String): ContainerCreator = {
    this.command = CommonUtils.requireNonEmpty(command)
    this
  }

  private[this] def assertNonEmpty[K, V](map: Map[K, V]): Map[K, V] = if (Objects.requireNonNull(map).isEmpty)
    throw new IllegalArgumentException("empty input!")
  else map
}
