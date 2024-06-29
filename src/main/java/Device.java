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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Device {
  private static Logger logger = LoggerFactory.getLogger(Device.class);
  public static final AtomicLong ID_COUNTER = new AtomicLong(0);
  private final String deviceId;
  private final List<String> measurementIds;
  private final List<TSDataType> types;
  private final boolean aligned;
  private final long interval;
  private final double freq;

  public Device(
      int measurementCount, TSDataType[] dataTypes, boolean aligned, long interval, double freq) {
    if (Configuration.mode.equalsIgnoreCase("iotdb")) {
      this.deviceId = Configuration.storageGroupName + ".d" + ID_COUNTER.getAndIncrement();
    } else {
      this.deviceId = "d" + ID_COUNTER.getAndIncrement();
    }
    this.aligned = aligned;
    this.interval = interval;
    this.measurementIds = new ArrayList<>(measurementCount);
    for (int i = 0; i < measurementCount; ++i) {
      this.measurementIds.add("s" + i);
    }
    this.types = Arrays.asList(dataTypes);
    this.freq = freq;
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

  public void createSchema() throws IoTDBConnectionException, StatementExecutionException {
    if (Configuration.mode.equalsIgnoreCase("iotdb")) {
      List<TSEncoding> encodings = new ArrayList<>(measurementIds.size());
      List<CompressionType> compressionTypes = new ArrayList<>(measurementIds.size());
      List<String> fullPaths = new ArrayList<>(measurementIds.size());
      for (String measurementId : measurementIds) {
        encodings.add(TSEncoding.PLAIN);
        compressionTypes.add(CompressionType.SNAPPY);
        fullPaths.add(deviceId + "." + measurementId);
      }
      GlobalSessionPool.getInstance()
          .createMultiTimeseries(fullPaths, types, encodings, compressionTypes);
    } else {
      TDEngineSessionPool.createSchema(this);
    }
  }

  public int getMeasurementCount() {
    return measurementIds.size();
  }

  public double getFreq() {
    return freq;
  }

  public String toTDengineSQL() {
    StringBuilder builder = new StringBuilder();
    builder.append("CREATE TABLE ").append(deviceId).append(" (ts timestamp,");
    for (int i = 0; i < measurementIds.size(); ++i) {
      String id = measurementIds.get(i);
      TSDataType type = types.get(i);
      builder.append(id).append(' ');
      switch (type) {
        case INT32:
        case INT64:
          builder.append("BIGINT");
          break;
        case FLOAT:
          builder.append("FLOAT");
          break;
        case DOUBLE:
          builder.append("DOUBLE");
          break;
        case BOOLEAN:
          builder.append("BOOL");
          break;
        case TEXT:
          builder.append("BINARY(1024)");
          break;
      }
      if (i != measurementIds.size() - 1) {
        builder.append(", ");
      }
    }
    return builder.append(");").toString();
  }
}
