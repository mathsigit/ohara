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

package com.island.ohara.kafka.connector.csv.sink;

import com.island.ohara.kafka.connector.csv.CsvSinkConfig;
import com.island.ohara.kafka.connector.csv.RecordWriterProvider;
import com.island.ohara.kafka.connector.storage.Storage;

public class CsvRecordWriterProvider implements RecordWriterProvider {
  private static final String EXTENSION = ".csv";

  private final Storage storage;

  public CsvRecordWriterProvider(Storage storage) {
    this.storage = storage;
  }

  public String getExtension() {
    return EXTENSION;
  }

  public CsvRecordWriter getRecordWriter(CsvSinkConfig config, String filePath) {
    return new CsvRecordWriter(config, filePath, storage);
  }
}
