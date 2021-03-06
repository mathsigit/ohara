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

import { omit } from 'lodash';
import PropTypes from 'prop-types';
import TextField from '@material-ui/core/TextField';

const PositiveShort = (props) => {
  const { input, meta = {}, helperText, refs, ...rest } = omit(props, [
    'tableKeys',
  ]);

  const { name, onChange, value, ...restInput } = omit(input, ['type']);

  const hasError = (meta.error && meta.touched) || (meta.error && meta.dirty);

  return (
    <TextField
      {...rest}
      InputProps={{ inputProps: { min: 0, max: 32767 }, ...restInput }}
      error={hasError}
      fullWidth
      helperText={hasError ? meta.error : helperText}
      id={name}
      name={name}
      onChange={onChange}
      ref={refs}
      type="number"
      value={value}
    />
  );
};

PositiveShort.propTypes = {
  input: PropTypes.shape({
    name: PropTypes.string.isRequired,
    onChange: PropTypes.func.isRequired,
    value: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.number,
      PropTypes.object,
    ]).isRequired,
  }).isRequired,
  meta: PropTypes.shape({
    dirty: PropTypes.bool,
    touched: PropTypes.bool,
    error: PropTypes.oneOfType([PropTypes.string, PropTypes.object]),
  }),
  width: PropTypes.string,
  helperText: PropTypes.oneOfType([PropTypes.string, PropTypes.object]),
  errorMessage: PropTypes.string,
  refs: PropTypes.object,
};

export default PositiveShort;
