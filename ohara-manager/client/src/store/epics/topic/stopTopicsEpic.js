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
import { merge } from 'lodash';
import { ofType } from 'redux-observable';
import {
  distinctUntilChanged,
  map,
  mergeMap,
  startWith,
  tap,
  takeUntil,
} from 'rxjs/operators';

import { fetchAndStopTopics } from 'observables';
import * as actions from 'store/actions';
import * as schema from 'store/schema';
import { catchErrorWithEventLog } from '../utils';

export default (action$) =>
  action$.pipe(
    ofType(actions.stopTopics.TRIGGER),
    map((action) => action.payload),
    distinctUntilChanged(),
    mergeMap(({ values, resolve, reject }) => {
      const { workspaceKey } = values;
      return fetchAndStopTopics(workspaceKey).pipe(
        tap((data) => {
          if (resolve) resolve(data);
          return data;
        }),
        map((data) => normalize(data, [schema.topic])),
        map((normalizedData) => actions.stopTopics.success(normalizedData)),
        startWith(actions.stopTopics.request()),
        catchErrorWithEventLog((err) => {
          if (reject) reject(err);
          return actions.stopTopics.failure(merge(err));
        }),
        takeUntil(action$.pipe(ofType(actions.stopTopics.CANCEL))),
      );
    }),
  );
