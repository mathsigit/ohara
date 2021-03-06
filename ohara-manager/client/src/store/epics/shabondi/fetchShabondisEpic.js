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

import { normalize } from 'normalizr';
import { ofType } from 'redux-observable';
import { defer } from 'rxjs';
import { map, startWith, switchMap } from 'rxjs/operators';

import * as shabondiApi from 'api/shabondiApi';
import * as actions from 'store/actions';
import * as schema from 'store/schema';
import { catchErrorWithEventLog } from '../utils';

export default (action$) => {
  return action$.pipe(
    ofType(actions.fetchShabondis.TRIGGER),
    switchMap(() =>
      defer(() => shabondiApi.getAll()).pipe(
        map((res) => normalize(res.data, [schema.shabondi])),
        map((normalizedData) => actions.fetchShabondis.success(normalizedData)),
        startWith(actions.fetchShabondis.request()),
        catchErrorWithEventLog((err) => actions.fetchShabondis.failure(err)),
      ),
    ),
  );
};
