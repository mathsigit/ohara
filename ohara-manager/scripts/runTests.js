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

const execa = require('execa');
const path = require('path');
const _ = require('lodash');
const { v4: uuid } = require('uuid');

const mergeReports = require('./mergeReports');
const { randomPort, logger } = require('../utils/commonUtils');
const {
  waitOnService,
  isBuildDirExist,
  buildCypressEnv,
  getDefaultEnv,
} = require('./scriptsUtils');
const {
  API_ROOT,
  PORT = 5050,
  nodeHost,
  nodePort,
  nodeUser,
  nodePass,
  servicePrefix,
  spec,
  ci = false,
} = require('../config');

/* eslint-disable no-process-exit */
const run = async (options) => {
  const { ci, apiRoot, clientPort, testMode, prefix } = options;
  const isRandom = options.serverPort === 0; // Use a random port when it's explicitly set to 0
  const serverPort = isRandom ? randomPort() : options.serverPort;

  let server;
  let client;
  let cypress;

  const SERVER_UID = `${prefix}-ohara-manager-${uuid().substr(0, 8)}`;
  const CLIENT_UID = `${prefix}-ohara-manager-client-${uuid().substr(0, 8)}`;
  const node = {
    nodeHost,
    nodePort,
    nodeUser,
    nodePass,
  };

  // Kill all processes when users exist with ctrl + c
  process.on('SIGINT', () => {
    killSubProcess();
    process.exit();
  });

  // Ensure all processes are killed before node exits
  process.on('exit', () => {
    killSubProcess();
    process.exit();
  });

  // Start ohara manager server
  logger.info('Starting ohara manager server');
  server = execa(
    'forever',
    [
      'start',
      '--uid',
      SERVER_UID,
      '--append',
      '--minUptime',
      1000,
      '--spinSleepTime',
      1000,
      'start.js',
      '--configurator',
      apiRoot,
      '--port',
      serverPort,
    ],
    {
      stdio: 'inherit',
    },
  );

  logger.log('Server pid', server.pid);

  try {
    await server;
  } catch (err) {
    logger.error(err.message);
    process.exit(1);
  }

  // Wait until the server is ready
  await waitOnService(`http://localhost:${serverPort}`);

  // Start client server for generating instrument code by instrument-cra plugin
  // This instrument plugin is only available in the development environment!
  // https://github.com/cypress-io/instrument-cra/issues/135
  if (ci && testMode !== 'api') {
    logger.info(`Starting client server`);
    client = execa(
      'forever',
      [
        '--uid',
        CLIENT_UID,
        '--append',
        '--minUptime',
        1000,
        '--spinSleepTime',
        1000,
        'start',
        '-c',
        'node -r @cypress/instrument-cra',
        'node_modules/react-scripts/scripts/start.js',
      ],
      {
        cwd: 'client',
        stdio: 'inherit',
        env: {
          BROWSER: 'none',
        },
      },
    );

    logger.log('Client pid', client.pid);

    try {
      await client;
    } catch (err) {
      logger.red(err.message);
      process.exit(1);
    }

    // Wait until the client dev server is ready
    await waitOnService(`http://localhost:${clientPort}`);
  }

  // We need these jars for test
  try {
    // --ci: indicate this command is running under QA mode
    execa.sync('yarn', ['copy:jars', '--ci'], {
      stdio: 'inherit',
    });
  } catch (err) {
    logger.error(err.message);
    process.exit(1);
  }

  // Run test
  logger.info(`Running ${testMode} tests with Cypress`);

  cypress = execa(
    'yarn',
    [
      `test:${testMode}:run`,
      '--config',
      `baseUrl=http://localhost:${ci ? clientPort : serverPort}`,
      // No docs for this yet. To specify a test spec: --spec `it/appBar/elements.test.ts` or --spec `e2e/pipeline.test.ts`
      ...(spec ? ['--spec', path.join('..', 'client', 'cypress', spec)] : []),
      '--env',
      buildCypressEnv({ node, clientPort, serverPort, prefix }),
    ],
    {
      cwd: 'client',
      stdio: 'inherit',
    },
  );
  logger.log('Cypress pid', cypress.pid);

  function killSubProcess() {
    try {
      if (cypress) cypress.kill();
      if (client) client.kill();
      if (server) server.kill();

      const processes = execa.sync('forever', ['list']).stdout.trim();
      const hasServer = processes.includes(SERVER_UID);
      const hasClient = processes.includes(CLIENT_UID);
      // Processes started with forever need to clean with forever command
      if (hasServer) execa.sync('forever', ['stop', SERVER_UID]);
      if (hasClient) execa.sync('forever', ['stop', CLIENT_UID]);
    } catch (err) {
      logger.error(err.message);
      process.exit(1);
    }
  }

  try {
    await cypress;
  } catch (err) {
    logger.error(err.message);
  } finally {
    await mergeReports(`client${_.capitalize(testMode)}`);
  }
};

const testMode = process.env.CYPRESS_TEST_MODE;
const defaultPrefix = getDefaultEnv().servicePrefix;
const prefix = servicePrefix ? servicePrefix : defaultPrefix || 'local';

// Do not run the test if the build dir is not present
// as this will cause the script to fail silently
if (!isBuildDirExist(testMode)) {
  process.exit(1);
}

run({
  ci,
  apiRoot: API_ROOT,
  serverPort: PORT,
  clientPort: 3000,
  testMode,
  prefix,
});
