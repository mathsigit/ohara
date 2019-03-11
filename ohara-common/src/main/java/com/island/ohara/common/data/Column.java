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

package com.island.ohara.common.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** implements Serializable ,because akka unmashaller throws java.io.NotSerializableException */
public final class Column extends Data implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String name;
  private final String newName;
  private final DataType dataType;
  private final int order;

  public static Column of(String name, String newName, DataType dataType, int order) {
    return new Column(name, newName, dataType, order);
  }

  public static Column of(String name, DataType dataType, int order) {
    return new Column(name, name, dataType, order);
  }

  private Column(String name, String newName, DataType dataType, int order) {
    this.name = name;
    this.newName = newName;
    this.dataType = dataType;
    this.order = order;
  }

  public String name() {
    return name;
  }

  public String newName() {
    return newName;
  }

  public DataType dataType() {
    return dataType;
  }

  public int order() {
    return order;
  }
  // kafka connector accept only Map[String, String] as input arguments so we have to serialize the
  // column to a string
  // TODO: Personally, I hate this ugly workaround...by chia
  public static final String COLUMN_KEY = "__row_connector_schema";

  /**
   * Column object serializes to String It uses "," to join all fields and concat Columns.
   *
   * @param schema Column list
   * @return a serialized string
   */
  public static String fromColumns(List<Column> schema) {

    return schema
        .stream()
        .map(c -> Arrays.asList(c.name, c.newName, c.dataType.name, String.valueOf(c.order)))
        .map(list -> String.join(",", list))
        .collect(Collectors.joining(","));
  }

  /**
   * Deserialized frome String It is split by ","
   *
   * @param columnsString generated by fromColumns
   * @return Column list
   */
  public static List<Column> toColumns(String columnsString) {
    if (columnsString == null || columnsString.isEmpty()) return Collections.emptyList();
    else {
      int tupleLength = 4;
      String[] splits = columnsString.split(",");
      if (splits.length % tupleLength != 0)
        throw new IllegalArgumentException(
            String.format("invalid format from columns string:%s", columnsString));
      else {
        return Stream.iterate(0, i -> i + tupleLength)
            .limit(splits.length / tupleLength)
            .map(
                x ->
                    new Column(
                        splits[x],
                        splits[x + 1],
                        DataType.of(splits[x + 2]),
                        Integer.parseInt(splits[x + 3])))
            .collect(Collectors.toList());
      }
    }
  }
}
