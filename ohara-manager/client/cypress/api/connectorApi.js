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

import * as utils from '../utils';

describe('Connector API', () => {
  let nodeName = '';
  let zookeeperClusterName = '';
  let brokerClusterName = '';
  let workerClusterName = '';
  let connectorName = '';
  let topicName = '';

  before(() => cy.deleteAllServices());

  beforeEach(() => {
    nodeName = `node${utils.makeRandomStr()}`;
    zookeeperClusterName = `zookeeper${utils.makeRandomStr()}`;
    brokerClusterName = `broker${utils.makeRandomStr()}`;
    workerClusterName = `worker${utils.makeRandomStr()}`;
    connectorName = `connector${utils.makeRandomStr()}`;
    topicName = `topic${utils.makeRandomStr()}`;

    cy.createNode({
      name: nodeName,
      port: 22,
      user: utils.makeRandomStr(),
      password: utils.makeRandomStr(),
    });

    cy.createZookeeper({
      name: zookeeperClusterName,
      nodeNames: [nodeName],
    });

    cy.startZookeeper(zookeeperClusterName);

    cy.createBroker({
      name: brokerClusterName,
      zookeeperClusterName,
      nodeNames: [nodeName],
    });

    cy.startBroker(brokerClusterName);

    cy.testCreateWorker({
      name: workerClusterName,
      brokerClusterName,
      nodeNames: [nodeName],
    });

    cy.testCreateTopic({
      name: topicName,
      brokerClusterName,
    });

    cy.createConnector({
      className: 'com.island.ohara.connector.ftp.FtpSource',
      'connector.name': connectorName,
      name: connectorName,
      topics: [topicName],
      workerClusterName,
    }).as('createConnector');
  });

  it('createConnector', () => {
    cy.get('@createConnector').then(res => {
      const {
        data: { isSuccess, result },
      } = res;
      const { settings } = result;

      expect(isSuccess).to.eq(true);

      expect(settings).to.be.a('object');
      expect(settings.className).to.be.a('string');
      expect(settings['connector.name']).to.eq(connectorName);
      expect(settings['tasks.max']).to.be.a('number');
      expect(settings.name).to.be.a('string');
      expect(settings.workerClusterName).to.be.a('string');
    });
  });

  it('fetchConnector', () => {
    cy.fetchConnector(connectorName).then(res => {
      const {
        data: { isSuccess, result },
      } = res;
      const { settings } = result;

      expect(isSuccess).to.eq(true);

      expect(result).to.include.keys('settings');
      expect(settings).to.be.a('object');
      expect(settings['className']).to.be.a('string');
      expect(settings['connector.name']).to.eq(connectorName);
      expect(settings['tasks.max']).to.be.a('number');
      expect(settings.name).to.be.a('string');
      expect(settings.workerClusterName).to.be.a('string');
    });
  });

  it('updateConnector', () => {
    const params = {
      name: connectorName,
      params: {
        name: connectorName,
        author: 'root',
        columns: [
          { dataType: 'STRING', name: 'test', newName: 'test', order: 1 },
        ],
        className: 'com.island.ohara.connector.ftp.FtpSource',
        'connector.name': 'Untitled source',
        'ftp.completed.folder': 'test',
        'ftp.encode': 'UTF-8',
        'ftp.error.folder': 'test',
        'ftp.hostname': 'test',
        'ftp.input.folder': 'test',
        'ftp.port': 20,
        'ftp.user.name': 'test',
        'ftp.user.password': 'test',
        kind: 'source',
        revision: '1e7da9544e6aa7ad2f9f2792ed8daf5380783727',
        'tasks.max': 1,
        topics: [topicName],
        version: '0.7.0-SNAPSHOT',
        workerClusterName,
      },
    };

    cy.updateConnector(params).then(res => {
      const {
        data: { isSuccess, result },
      } = res;
      const { settings } = result;

      expect(isSuccess).to.eq(true);

      expect(settings).to.be.a('object');
      expect(settings.author).to.be.a('string');
      expect(settings.columns).to.be.a('array');
      expect(settings.className).to.be.a('string');
      expect(settings.name).to.be.a('string');
      expect(settings['connector.name']).to.be.a('string');
      expect(settings['ftp.completed.folder']).to.be.a('string');
      expect(settings['ftp.encode']).to.be.a('string');
      expect(settings['ftp.error.folder']).to.be.a('string');
      expect(settings['ftp.hostname']).to.be.a('string');
      expect(settings['ftp.input.folder']).to.be.a('string');
      expect(settings['ftp.port']).to.be.a('number');
      expect(settings['ftp.user.name']).to.be.a('string');
      expect(settings['ftp.user.password']).to.be.a('string');
      expect(settings.kind).to.be.a('string');
      expect(settings.revision).to.be.a('string');
      expect(settings['tasks.max']).to.be.a('number');
      expect(settings.topics).to.be.a('array');
      expect(settings.version).to.be.a('string');
      expect(settings.workerClusterName).to.be.a('string');
    });
  });

  it('startConnector', () => {
    cy.startConnector(connectorName).then(res => {
      const { data } = res;
      expect(data.isSuccess).to.eq(true);
      expect(data.result).to.include.keys('settings', 'state');
      expect(data.result.state).to.be.a('string');
    });
  });

  it('stopConnector', () => {
    cy.stopConnector(connectorName).then(res => {
      const { data } = res;
      expect(data.isSuccess).to.eq(true);
      expect(data.result).to.include.keys('settings');
    });
  });

  it('deleteConnector', () => {
    cy.deleteConnector(connectorName).then(res => {
      const { data } = res;
      expect(data.isSuccess).to.eq(true);
    });
  });
});
