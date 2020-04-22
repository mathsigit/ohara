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

import React from 'react';

import SettingsMain from './SettingsMain';
import SettingsMenu from './SettingsMenu';
import { SETTINGS_COMPONENT_TYPES } from 'const';
import { useEditWorkspaceDialog } from 'context';
import { Wrapper, StyledFullScreenDialog } from './SettingsStyles';
import { useConfig } from './SettingsHooks';

const Settings = () => {
  const { isOpen, close } = useEditWorkspaceDialog();
  const { menu, sections } = useConfig();
  const [selectedMenu, setSelectedMenu] = React.useState('');
  const [selectedComponent, setSelectedComponent] = React.useState(null);
  const [scrollRef, setScrollRef] = React.useState(null);

  const handleMenuClick = newMenuItem => {
    setSelectedMenu(newMenuItem);
  };

  const handleComponentChange = newPage => {
    setSelectedComponent(newPage);
  };

  const resetSelectedItem = () => {
    setScrollRef(selectedComponent.ref);
    setSelectedComponent(null);
  };

  // Use a different layout for rendering page component
  const isPageComponent =
    !!selectedComponent?.name &&
    selectedComponent?.type === SETTINGS_COMPONENT_TYPES.PAGE;

  React.useEffect(() => {
    if (!isPageComponent && scrollRef?.current) {
      scrollRef.current.scrollIntoView();
    }
  }, [isPageComponent, scrollRef, selectedComponent]);

  return (
    <StyledFullScreenDialog
      title="Settings"
      open={isOpen}
      handleClose={close}
      testId="edit-workspace-dialog"
      isPageComponent={isPageComponent}
    >
      <Wrapper>
        <SettingsMenu
          menu={menu}
          selected={selectedMenu}
          handleClick={handleMenuClick}
          closePageComponent={resetSelectedItem}
          isPageComponent={isPageComponent}
          setScrollRef={setScrollRef}
        />
        <SettingsMain
          sections={sections}
          handleChange={handleComponentChange}
          handleClose={resetSelectedItem}
          selected={selectedComponent}
        />
      </Wrapper>
    </StyledFullScreenDialog>
  );
};

export default Settings;