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
  private long lastSubmitTime = 0L;
  private final DataQueue queue = DataQueue.getInstance();

  public CSVLoader(Path csvDirectory) {
    this.csvDirectory = csvDirectory;
    this.autoAISchema = buildSchema();
  }

  public List<MeasurementSchema> buildSchema() {
    List<MeasurementSchema> autoAISchema = new ArrayList<>();
    autoAISchema.add(new MeasurementSchema("AccelerationFB", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("AccelerationLR", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("Speed_TypeA", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("SteeringAngle_TypeA", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("EngineRPM_TypeA", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TirePressureFL_kpa", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("latitude", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("longitude", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("AccelPedalAngle_TypeA", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TirePressureFR_kpa", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TirePressureRL_kpa", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TirePressureRR_kpa", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("AmbientTemperature", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("TemperatureD", TSDataType.DOUBLE));
    autoAISchema.add(new MeasurementSchema("FuelGageIndication", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("TurnLampSwitchStatus", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("ATShiftPosition", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("BrakePedal", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("DoorOpenD", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("ParkingBrake", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("EcoModeIndicator", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("PowerModeSelect_TypeA", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("SportModeSelect", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("WindowPositionD", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("AirConIndicator", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("Odometer_km", TSDataType.INT32));
    autoAISchema.add(new MeasurementSchema("HeadLamp_TypeB", TSDataType.INT32));
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
    if (tablet == null) {
      return;
    }
    long lastTimeStamp = tablet.timestamps[tablet.rowSize - 1];
    long interval = lastSubmitTime == 0 ? 0 : Long.max(0, lastTimeStamp - lastSubmitTime);
    try {
      logger.debug(
          "Sleeping for {} ms after submit tablet, lastTimeStamp: {}", interval, lastTimeStamp);
      Thread.sleep(interval);
    } catch (InterruptedException e) {
      logger.error("Error sleeping after submit tablet:", e);
    }
    if (!queue.submit(tablet) && System.currentTimeMillis() - lastLogTime > 10_000) {
      logger.error("Failed to submit tablet to the queue for device {}", tablet.deviceId);
      lastLogTime = System.currentTimeMillis();
    }
    lastSubmitTime = Long.max(lastSubmitTime, lastTimeStamp);
  }

  @Override
  public void run() {
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
          List<Long> timestamps = new ArrayList<>();
          Object[] values = buildValues();
          int rowSize = 0;
          int lineCount = 0;
          while ((line = reader.readLine()) != null) {
            if (skipFirstLine) {
              skipFirstLine = false;
              continue;
            }
            String[] measurements = line.split(",");
            if (measurements.length != autoAISchema.size() + 2) {
              logger.error(
                  "The number of values in the line is not equal to the number of columns in the schema");
              continue;
            }
            long timestamp = Long.parseLong(measurements[0].trim());
            String deviceId = measurements[1];
            if (lastDeviceId == null) {
              lastDeviceId = deviceId;
            }
            if (!lastDeviceId.equals(deviceId)) {
              Tablet tablet = buildTablet(lastDeviceId, autoAISchema, timestamps, values, rowSize);
              submitTablet(tablet);
              lastDeviceId = deviceId;
              timestamps = new ArrayList<>();
              values = buildValues();
              rowSize = 0;
            }
            for (int i = 0; i < autoAISchema.size(); i++) {
              if (!measurements[i + 2].equals("null")) {
                if (autoAISchema.get(i).getType() == TSDataType.INT32) {
                  ((List<Integer>) values[i]).add(Integer.parseInt(measurements[i + 2]));
                } else {
                  ((List<Double>) values[i]).add(Double.parseDouble(measurements[i + 2]));
                }
              }
            }
            timestamps.add(timestamp);
            rowSize++;
            lineCount++;
            if (lineCount % 1000 == 0) {
              logger.info("Processed {} lines in file {}", lineCount, file);
            }
          }
          Tablet tablet = buildTablet(lastDeviceId, autoAISchema, timestamps, values, rowSize);
          submitTablet(tablet);
        } catch (IOException e) {
          logger.error("Error reading file {}", file, e);
        }
      }
    } catch (IOException e) {
      logger.error("Error listing files in directory {}", csvDirectory, e);
    }
    logger.info("Finished loading CSV files from directory {}", csvDirectory);
  }

  private Tablet buildTablet(
      String lastDeviceId,
      List<MeasurementSchema> autoAISchema,
      List<Long> timestamps,
      Object[] values,
      int rowSize) {
    if (rowSize == 0) {
      return null;
    }
    long[] timestampsArray = timestamps.stream().mapToLong(Long::longValue).toArray();
    Object[] valuesArray = new Object[autoAISchema.size()];
    for (int i = 0; i < autoAISchema.size(); i++) {
      if (autoAISchema.get(i).getType() == TSDataType.INT32) {
        valuesArray[i] = ((List<Integer>) values[i]).stream().mapToInt(Integer::intValue).toArray();
      } else {
        valuesArray[i] =
            ((List<Double>) values[i]).stream().mapToDouble(Double::doubleValue).toArray();
      }
    }
    Tablet tablet =
        new Tablet(lastDeviceId, autoAISchema, timestampsArray, valuesArray, null, rowSize);
    return tablet;
  }
}
