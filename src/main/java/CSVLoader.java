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
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CSVLoader implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(CSVLoader.class);
  private final Path csvDirectory;
  private final List<MeasurementSchema> autoAISchema;
  private long lastLogTime = 0L;
  private final DataQueue queue = DataQueue.getInstance();

  public CSVLoader(Path csvDirectory) {
    this.csvDirectory = csvDirectory;
    this.autoAISchema = buildSchema();
  }

  public List<MeasurementSchema> buildSchema() {
    List<MeasurementSchema> autoAISchema = new ArrayList<>();
    autoAISchema.add(new MeasurementSchema("AccelerationFB(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("AccelerationLR(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("Speed_TypeA(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("SteeringAngle_TypeA(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("EngineRPM_TypeA(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TirePressureFL_kpa(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("latitude(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("longitude(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("AccelPedalAngle_TypeA(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TirePressureFR_kpa(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TirePressureRL_kpa(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TirePressureRR_kpa(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("AmbientTemperature(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TemperatureD(DOUBLE)", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("FuelGageIndication(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("TurnLampSwitchStatus(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("ATShiftPosition(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("BrakePedal(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("DoorOpenD(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("ParkingBrake(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("EcoModeIndicator(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("PowerModeSelect_TypeA(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("SportModeSelect(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("WindowPositionD(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("AirConIndicator(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("Odometer_km(INT32)", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("HeadLamp_TypeB(INT32)", TSDataType.INT32));
    return autoAISchema;
  }

  public Object[] buildValues() {
    Object[] values = new Object[autoAISchema.size()];
    for (int i = 0; i < autoAISchema.size(); i++) {
      if (autoAISchema.get(i).getType() == TSDataType.INT32) {
        values[i] = new ArrayList<Integer>();
      } else {
        values[i] = new ArrayList<Double>();
      }
    }
    return values;
  }

  public void submitTablet(Tablet tablet) {
    if (!queue.submit(tablet) && System.currentTimeMillis() - lastLogTime > 10_000) {
      logger.error("Failed to submit tablet to the queue for device {}", tablet.deviceId);
      lastLogTime = System.currentTimeMillis();
    }
    try {
      Thread.sleep(Configuration.loadCSVIntervalInMs);
    } catch (InterruptedException e) {
      logger.error("Error sleeping after submit tablet:", e);
    }
  }

  @Override
  public void run() {
    while (true) {
      long currentTime = System.currentTimeMillis();
      List<MeasurementSchema> autoAISchema = buildSchema();
      try {
        List<String> files =
            Files.list(csvDirectory)
                .filter(Files::isRegularFile)
                .map(Path::toString)
                .collect(Collectors.toList());
        for (String file : files) {
          try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            boolean skipFirstLine = true;
            String lastDeviceId = null;
            long[] timestamps = new long[autoAISchema.size()];
            Object[] values = buildValues();
            BitMap[] bitMaps = new BitMap[autoAISchema.size()];
            int rowSize = 0;
            boolean isAligned = true;

            while ((line = reader.readLine()) != null) {
              if (skipFirstLine) {
                skipFirstLine = false;
                continue;
              }
              String[] measurements = line.split(",");
              if (measurements.length != autoAISchema.size() + 1) {
                logger.error(
                    "The number of values in the line is not equal to the number of columns in the schema");
                continue;
              }
              String deviceId = measurements[0];
              if (lastDeviceId == null) {
                lastDeviceId = deviceId;
              }
              if (!lastDeviceId.equals(deviceId)) {
                Tablet tablet =
                    new Tablet(lastDeviceId, autoAISchema, timestamps, values, bitMaps, 1024);
                submitTablet(tablet);
                lastDeviceId = deviceId;
                timestamps = new long[autoAISchema.size()];
                values = buildValues();
                bitMaps = new BitMap[autoAISchema.size()];
                rowSize = 0;
              }
              for (int i = 0; i < autoAISchema.size(); i++) {
                if (!measurements[i + 1].equals("null")) {
                  bitMaps[i].mark(rowSize);
                  if (autoAISchema.get(i).getType() == TSDataType.INT32) {
                    ((List<Integer>) values[i]).add(Integer.parseInt(measurements[i + 1]));
                  } else {
                    ((List<Double>) values[i]).add(Double.parseDouble(measurements[i + 1]));
                  }
                }
              }
              timestamps[rowSize] = currentTime;
              rowSize++;
            }
            Tablet tablet =
                new Tablet(lastDeviceId, autoAISchema, timestamps, values, bitMaps, 1024);
            submitTablet(tablet);
          } catch (IOException e) {
            logger.error("Error reading file {}", file, e);
          }
        }
      } catch (IOException e) {
        logger.error("Error listing files in directory {}", csvDirectory, e);
      }
    }
  }
}
