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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Device {
  public static final AtomicLong ID_COUNTER = new AtomicLong(0);
  private final String deviceId;
  private final List<String> measurementIds;
  private final List<TSDataType> types;
  private final boolean aligned;
  private final long interval;

  public Device(int measurementCount, TSDataType[] dataTypes, boolean aligned, long interval) {
    this.deviceId = Configuration.storageGroupName + ".d" + ID_COUNTER.getAndIncrement();
    this.aligned = aligned;
    this.interval = interval;
    this.measurementIds = new ArrayList<>(measurementCount);
    for (int i = 0; i < measurementCount; ++i) {
      this.measurementIds.add("s" + i);
    }
    this.types = Arrays.asList(dataTypes);
  }

  public long getInterval() {
    return interval;
  }

  public Record generateData() {
    List<Object> values = new ArrayList<>(types.size());
    for (TSDataType type : types) {
      switch (type) {
        case BOOLEAN:
          values.add(ValuePool.getInstance().getBoolean());
          break;
        case INT32:
          values.add(ValuePool.getInstance().getInt());
          break;
        case INT64:
          values.add(ValuePool.getInstance().getLong());
          break;
        case FLOAT:
          values.add(ValuePool.getInstance().getFloat());
          break;
        case DOUBLE:
          values.add(ValuePool.getInstance().getDouble());
          break;
        case TEXT:
          values.add(ValuePool.getInstance().getText());
          break;
      }
    }
    return new Record(deviceId, System.currentTimeMillis(), measurementIds, types, values, aligned);
  }

  public String getDeviceId() {
    return deviceId;
  }

  public List<String> getSchemaSql() {
    String pattern = "CREATE TIMESERIES %s.%s WITH DATATYPE=%s;";
    List<String> sqls = new ArrayList<>(measurementIds.size());
    for (int i = 0; i < measurementIds.size(); ++i) {
      sqls.add(String.format(pattern, deviceId, measurementIds.get(i), types.get(i)));
    }
    return sqls;
  }
}
