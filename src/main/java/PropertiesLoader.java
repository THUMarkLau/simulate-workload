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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropertiesLoader {
  private static final Logger logger = LoggerFactory.getLogger(PropertiesLoader.class);
  private final File configFile;

  public PropertiesLoader(File file) {
    this.configFile = file;
  }

  public List<Path> loadCSV() throws IOException {
    logger.info("Loading {}", configFile.getAbsolutePath());
    List<Path> csvDirectories = new ArrayList<>();
    String line;
    try (BufferedReader reader = new BufferedReader(new FileReader(configFile))) {
      while ((line = reader.readLine()) != null) {
        csvDirectories.add(new File(line).toPath());
      }
    }
    return csvDirectories;
  }

  public Map<Long, List<Device>> load() throws IOException {
    logger.info("Loading {}", configFile.getAbsolutePath());
    List<Device> devices = new ArrayList<>();
    int measurementTotalCount = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(configFile))) {
      int deviceCount = Integer.parseInt(reader.readLine());
      String line;
      for (int i = 0; i < deviceCount; ++i) {
        line = reader.readLine();
        if (line == null) {
          System.out.println("Error: not enough device properties in config file.");
        }
        // parse line
        String[] properties = line.split(" ");
        int measurementCount = Integer.parseInt(properties[0]);
        measurementTotalCount += measurementCount;
        double freq = Double.parseDouble(properties[1]);
        boolean aligned = Boolean.parseBoolean(properties[2]);
        TSDataType[] types = new TSDataType[measurementCount];
        for (int j = 0; j < measurementCount; j++) {
          byte type = Byte.parseByte(properties[j + 3]);
          types[j] = TSDataType.deserialize(type);
        }
        devices.add(new Device(measurementCount, types, aligned, (long) (1000 / freq), freq));
      }
      logger.info("Total measurements: {}", measurementTotalCount);
    }
    SchemaRegisterTask.totalCount.set(devices.size());
    devices.sort(Comparator.comparingLong(Device::getInterval));
    Map<Long, List<Device>> intervalDeviceMap = new HashMap<>();
    long currentInterval = 0;
    for (Device device : devices) {
      if (currentInterval == 0 || device.getInterval() - currentInterval >= 200) {
        currentInterval = device.getInterval();
      }
      intervalDeviceMap.computeIfAbsent(currentInterval, k -> new ArrayList<>()).add(device);
    }
    logger.info(
        "There are {} intervals in total, they are {} ms",
        intervalDeviceMap.size(),
        intervalDeviceMap.keySet());
    return intervalDeviceMap;
  }
}
