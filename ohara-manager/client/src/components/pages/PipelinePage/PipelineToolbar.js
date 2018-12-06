import React from 'react';
import PropTypes from 'prop-types';
import styled from 'styled-components';

import * as _ from 'utils/commonUtils';
import { Modal } from 'common/Modal';
import { DataTable } from 'common/Table';
import { update, fetchCluster } from 'utils/pipelineToolbarUtils';
import * as PIPELINES from 'constants/pipelines';
import {
  lightestBlue,
  lighterBlue,
  lightBlue,
  radiusNormal,
  durationNormal,
  trBgColor,
  blue,
} from 'theme/variables';

const ToolbarWrapper = styled.div`
  margin-bottom: 15px;
  padding: 15px 30px;
  border: 1px solid ${lightestBlue};
  border-radius: ${radiusNormal};
  display: flex;
  align-items: center;
`;

ToolbarWrapper.displayName = 'ToolbarWrapper';

const TableWrapper = styled.div`
  margin: 30px 30px 40px;
`;

const Table = styled(DataTable)`
  thead th {
    color: ${lightBlue};
    font-weight: normal;
  }

  td {
    color: ${lighterBlue};
  }

  tbody tr {
    cursor: pointer;
  }

  .is-active {
    background-color: ${trBgColor};
  }
`;

const Icon = styled.i`
  color: ${lighterBlue};
  font-size: 25px;
  margin-right: 20px;
  transition: ${durationNormal} all;
  cursor: pointer;

  &:hover,
  &.is-active {
    transition: ${durationNormal} all;
    color: ${blue};
  }

  &:last-child {
    border-right: none;
    margin-right: 0;
  }
`;

Icon.displayName = 'Icon';

const FileSavingStatus = styled.div`
  margin-left: auto;
  color: red;
  font-size: 12px;
  color: ${lighterBlue};
`;

FileSavingStatus.displayName = 'FileSavingStatus';

class PipelineToolbar extends React.Component {
  static propTypes = {
    match: PropTypes.shape({
      isExact: PropTypes.bool,
      params: PropTypes.object,
      path: PropTypes.string,
      url: PropTypes.string,
    }).isRequired,
    graph: PropTypes.arrayOf(
      PropTypes.shape({
        type: PropTypes.string,
        uuid: PropTypes.string,
        isActive: PropTypes.bool,
        isExact: PropTypes.bool,
        icon: PropTypes.string,
      }),
    ).isRequired,
    updateGraph: PropTypes.func.isRequired,
    hasChanges: PropTypes.bool.isRequired,
  };

  state = {
    isModalActive: false,
    sources: [],
    sinks: [],
    activeConnector: {},
    connectorType: '',
  };

  componentDidMount() {
    this.fetchCluster();
  }

  fetchCluster = async () => {
    const res = await fetchCluster();

    const sources = res.sources.filter(
      source => !PIPELINES.CONNECTOR_FILTERS.includes(source.className),
    );

    const sinks = res.sinks.filter(
      sink => !PIPELINES.CONNECTOR_FILTERS.includes(sink.className),
    );

    this.setState({ sources, sinks });
  };

  setDefaultConnector = connectorType => {
    this.setState({ activeConnector: this.state[connectorType][0] });
  };

  update = () => {
    const { updateGraph, graph } = this.props;
    const { activeConnector: connector } = this.state;
    update({ graph, updateGraph, connector });
  };

  handleModalOpen = connectorType => {
    this.setState({ isModalActive: true, connectorType }, () => {
      this.setDefaultConnector(this.state.connectorType);
    });
  };

  handleModalClose = () => {
    this.setState({ isModalActive: false });
  };

  handleConfirm = () => {
    this.update();
    this.handleModalClose();
  };

  handleTrSelect = name => {
    this.setState(prevState => {
      const { connectorType } = prevState;
      const active = prevState[connectorType].filter(
        connector => connector.className === name,
      );
      return {
        activeConnector: active[0],
      };
    });
  };

  render() {
    const { hasChanges } = this.props;
    const { ftpSource } = PIPELINES.CONNECTOR_KEYS;
    const { isModalActive, connectorType, activeConnector } = this.state;

    const connectors = this.state[connectorType];
    const _connectorType = connectorType.substring(0, connectorType.length - 1);
    const modalTitle = `Add a new ${_connectorType} connector`;

    return (
      <ToolbarWrapper>
        {!_.isEmptyStr(connectorType) && (
          <Modal
            title={modalTitle}
            isActive={isModalActive}
            width="600px"
            handleCancel={this.handleModalClose}
            handleConfirm={this.handleConfirm}
            confirmBtnText="Add"
          >
            <TableWrapper>
              <Table headers={PIPELINES.TABLE_HEADERS}>
                {connectors.map(({ className: name, version, revision }) => {
                  const isActive =
                    name === activeConnector.className ? 'is-active' : '';
                  return (
                    <tr
                      className={isActive}
                      key={name}
                      onClick={() => this.handleTrSelect(name)}
                    >
                      <td>{name}</td>
                      <td>{version}</td>
                      <td>{revision}</td>
                    </tr>
                  );
                })}
              </Table>
            </TableWrapper>
          </Modal>
        )}

        <Icon
          className="fas fa-file-import"
          onClick={() => this.handleModalOpen('sources')}
          data-id={ftpSource}
          data-testid="toolbar-sources"
        />
        <Icon className="fas fa-list-ul" data-testid="toolbar-topics" />
        <Icon className="fas fa-wind" data-testid="toolbar-streams" />
        <Icon
          className="fas fa-file-export"
          onClick={() => this.handleModalOpen('sinks')}
          data-id={ftpSource}
          data-testid="toolbar-sinks"
        />

        <FileSavingStatus>
          {hasChanges ? 'Saving...' : 'All changes saved'}
        </FileSavingStatus>
      </ToolbarWrapper>
    );
  }
}

export default PipelineToolbar;
