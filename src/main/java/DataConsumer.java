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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class DataConsumer implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(DataConsumer.class);
  public static final AtomicLong pointsCounter = new AtomicLong(0);
  public static final AtomicLong totalCount = new AtomicLong(0);
  private int requestSize = Configuration.requestSize;
  private List<Record> records;

  public DataConsumer() {
    records = new ArrayList<>(requestSize);
  }

  @Override
  public void run() {

    DataQueue queue = DataQueue.getInstance();
    long timeout = (long) (Configuration.timeout * 1000);
    while (true) {
      records.clear();
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < requestSize; i++) {
        try {
          Record data = queue.consume(timeout - (System.currentTimeMillis() - startTime));
          if (Objects.isNull(data)) {
            break;
          } else {
            records.add(data);
          }
        } catch (InterruptedException e) {
          logger.error("Error consuming data", e);
          return;
        }
      }

      try {
        sendRequest();
      } catch (Exception e) {
        logger.error("Meets error when sending request", e);
      }
    }
  }

  private void sendRequest() throws IoTDBConnectionException, StatementExecutionException {
    if (records.isEmpty()) {
      return;
    }
    if (Configuration.mode.equalsIgnoreCase("iotdb")) {
      List<String> deviceIds = new ArrayList<>(records.size());
      List<Long> timestamps = new ArrayList<>(records.size());
      List<List<String>> measurementIds = new ArrayList<>(records.size());
      List<List<TSDataType>> types = new ArrayList<>(records.size());
      List<List<Object>> values = new ArrayList<>(records.size());

      for (Record record : records) {
        deviceIds.add(record.deviceId);
        timestamps.add(record.timestamp);
        measurementIds.add(record.measurements);
        pointsCounter.addAndGet(record.measurements.size());
        types.add(record.types);
        values.add(record.values);
      }

      GlobalSessionPool.getInstance()
          .insertRecords(deviceIds, timestamps, measurementIds, types, values);
    } else {
      TDEngineSessionPool.sendRequest(records);
    }
  }
}
