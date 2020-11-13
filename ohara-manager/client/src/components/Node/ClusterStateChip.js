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

import PropTypes from 'prop-types';
import { capitalize } from 'lodash';

import Chip from '@material-ui/core/Chip';
import CheckCircleIcon from '@material-ui/icons/CheckCircle';
import HelpIcon from '@material-ui/icons/Help';
import { SERVICE_STATE } from 'api/apiInterface/clusterInterface';

const ClusterStateChip = ({ cluster }) => {
  if (cluster?.state === SERVICE_STATE.RUNNING) {
    return (
      <Chip
        color="primary"
        icon={<CheckCircleIcon />}
        label={capitalize(cluster.state)}
        size="small"
        variant="outlined"
      />
    );
  } else {
    return (
      <Chip
        icon={<HelpIcon />}
        label={cluster?.state ? capitalize(cluster.state) : 'Unknown'}
        size="small"
        variant="outlined"
      />
    );
  }
};

ClusterStateChip.propTypes = {
  cluster: PropTypes.shape({
    error: PropTypes.string,
    state: PropTypes.string,
  }).isRequired,
};

ClusterStateChip.defaultProps = {
  node: null,
};

export default ClusterStateChip;
