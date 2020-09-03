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

package oharastream.ohara.it.connector.jdbc
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable

@EnabledIfEnvironmentVariable(named = "ohara.it.oracle.db.url", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.oracle.db.username", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.oracle.db.password", matches = ".*")
class TestOracleJDBCSourceConnector extends BasicTestConnectorCollie {
  private[this] val DB_URL_KEY: String       = "ohara.it.oracle.db.url"
  private[this] val DB_USER_NAME_KEY: String = "ohara.it.oracle.db.username"
  private[this] val DB_PASSWORD_KEY: String  = "ohara.it.oracle.db.password"
  override protected def dbUrl: String       = sys.env(DB_URL_KEY)

  override protected def dbUserName: String = sys.env(DB_USER_NAME_KEY)

  override protected def dbPassword: String = sys.env(DB_PASSWORD_KEY)

  override protected def dbName: String = "oracle"

  override protected val tableName: String = s"TABLE${CommonUtils.randomString(5)}".toUpperCase

  override protected def jdbcDriverJarFileName: String = "ojdbc8.jar"

  override protected val columnPrefixName: String = "COLUMN"

  override protected val BINARY_TYPE_NAME: String = "RAW(30)"
}
