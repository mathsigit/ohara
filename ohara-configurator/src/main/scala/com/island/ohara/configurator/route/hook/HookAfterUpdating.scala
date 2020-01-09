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

package com.island.ohara.configurator.route.hook

import com.island.ohara.client.configurator.Data

import scala.concurrent.Future

/**
  * this hook is invoked after the object is updated and then added to store.
  *
  * @tparam Res result to response
  */
trait HookAfterUpdating[Res <: Data] {
  def apply(data: Res): Future[Res]
}
