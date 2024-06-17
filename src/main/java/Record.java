/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class Record {
  public final String deviceId;
  public final long timestamp;
  public final List<String> measurements;
  public final List<TSDataType> types;
  public final List<Object> values;
  public final boolean aligned;

  public Record(
      String deviceId,
      long timestamp,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values,
      boolean aligned) {
    this.deviceId = deviceId;
    this.timestamp = timestamp;
    this.measurements = measurements;
    this.types = types;
    this.values = values;
    this.aligned = aligned;
  }

  public String toTDengineSQL() {
    StringBuilder builder = new StringBuilder();
    builder.append(deviceId).append(" VALUES (").append(timestamp).append(',');
    for (Object value : values) {
      builder.append(value.toString());
      builder.append(',');
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append(") ");
    return builder.toString();
  }
}
