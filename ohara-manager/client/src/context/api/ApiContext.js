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

import React, { createContext, useContext, useMemo } from 'react';
import PropTypes from 'prop-types';

import { useApp } from 'context';
import { createApi as createConnectorApi } from './connectorApi';
import { createApi as createPipelineApi } from './pipelineApi';

const ApiContext = createContext();

const ApiProvider = ({ children }) => {
  const { connectorGroup, pipelineGroup, topicGroup, workerKey } = useApp();

  const connectorApi = useMemo(
    () => createConnectorApi({ connectorGroup, workerKey, topicGroup }),
    [connectorGroup, workerKey, topicGroup],
  );

  const pipelineApi = useMemo(() => createPipelineApi({ pipelineGroup }), [
    pipelineGroup,
  ]);

  return (
    <ApiContext.Provider
      value={{
        connectorApi,
        pipelineApi,
      }}
    >
      {children}
    </ApiContext.Provider>
  );
};

const useApi = () => {
  const context = useContext(ApiContext);
  if (context === undefined) {
    throw new Error('useApi must be used within a ApiProvider');
  }

  return context;
};

ApiProvider.propTypes = {
  children: PropTypes.node.isRequired,
};

export { ApiProvider, useApi };
