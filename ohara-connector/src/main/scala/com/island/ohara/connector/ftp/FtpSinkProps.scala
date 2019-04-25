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

package com.island.ohara.connector.ftp

case class FtpSinkProps(outputFolder: String,
                        needHeader: Boolean,
                        encode: String,
                        hostname: String,
                        port: Int,
                        user: String,
                        password: String) {
  def toMap: Map[String, String] = Map(
    FTP_OUTPUT -> outputFolder,
    FTP_NEED_HEADER -> needHeader.toString,
    FTP_ENCODE -> encode,
    FTP_HOSTNAME -> hostname,
    FTP_PORT -> port.toString,
    FTP_USER_NAME -> user,
    FTP_PASSWORD -> password
  ).filter(_._2.nonEmpty)
}

object FtpSinkProps {
  def apply(props: Map[String, String]): FtpSinkProps = FtpSinkProps(
    outputFolder = props(FTP_OUTPUT),
    needHeader = props(FTP_NEED_HEADER).toBoolean,
    encode = props(FTP_ENCODE),
    hostname = props(FTP_HOSTNAME),
    port = props(FTP_PORT).toInt,
    user = props(FTP_USER_NAME),
    password = props(FTP_PASSWORD)
  )
}
