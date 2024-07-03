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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);
  private static ExecutorService writeService;
  private static ExecutorService consumerService;
  private static Map<Long, List<Device>> intervalDeviceMap;
  private static List<Path> csvDirectories;

  public static void main(String[] args) throws Exception {
    loadConfig(args);
    initConsumerService();
    initSessionPool();
    startMonitor();
    if (Configuration.registerSchema) {
      registerSchema(intervalDeviceMap.values());
    }
    if (!Configuration.registerSchemaOnly) {
      startWriting();
      waitEnding();
    }
  }

  private static void loadConfig(String[] args) throws IOException {
    Configuration.parseConfig(args);
    if (!Configuration.loadCSV) {
      loadTimeSeriesConfig();
    } else {
      loadCSVConfig();
    }
  }

  private static void loadCSVConfig() throws IOException {
    PropertiesLoader propertiesLoader = new PropertiesLoader(new File(Configuration.configFile));
    csvDirectories = propertiesLoader.loadCSV();
    Configuration.queueSize = Math.max(csvDirectories.size() * 16, Configuration.queueSize);
  }

  private static void loadTimeSeriesConfig() throws IOException {
    PropertiesLoader propertiesLoader = new PropertiesLoader(new File(Configuration.configFile));
    intervalDeviceMap = propertiesLoader.load();
    Configuration.queueSize =
        Math.max(SchemaRegisterTask.totalCount.get() * 16, Configuration.queueSize);
  }

  private static void initConsumerService() {
    // start threads for consuming
    consumerService =
        Executors.newFixedThreadPool(
            Configuration.clientCount,
            new ThreadFactory() {
              private final AtomicInteger counter = new AtomicInteger(0);

              @Override
              public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("DataConsumer-" + counter.incrementAndGet());
                return thread;
              }
            });
    for (int i = 0; i < Configuration.clientCount; i++) {
      consumerService.submit(new DataConsumer());
    }
  }

  private static void initSessionPool() {
    if (Configuration.mode.equalsIgnoreCase("iotdb")) {
      GlobalSessionPool.getInstance().init();
      if (Configuration.clearBeforeStart) {
        try {
          GlobalSessionPool.getInstance().executeNonQueryStatement("delete database root.**");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          // ignore
        }
      }
    } else if (Configuration.mode.equalsIgnoreCase("tdengine")) {
      TDEngineSessionPool.init();
    }
  }

  private static void startMonitor() {
    if (Configuration.enableMonitor) {
      Thread monitorThread = new Thread(new Monitor());
      monitorThread.setName("MonitorThread");
      monitorThread.start();
    }
  }

  private static void registerSchema(Collection<List<Device>> devicesSet)
      throws InterruptedException {
    ExecutorService registerSchemaService = Executors.newFixedThreadPool(Configuration.clientCount);
    for (List<Device> devices : devicesSet) {
      for (Device device : devices) {
        registerSchemaService.submit(new SchemaRegisterTask(device));
      }
    }

    while (SchemaRegisterTask.finishedCount.get() < SchemaRegisterTask.totalCount.get()) {
      Thread.sleep(10000);
    }
    registerSchemaService.shutdownNow();
    registerSchemaService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    logger.info("Register schema end");
  }

  private static void startWriting() {
    if (Configuration.loadCSV) {
      startLoadingCSV();
    } else {
      startGenerating();
    }
  }

  private static void setSchemaTemplate() {
    if (Configuration.mode.equalsIgnoreCase("iotdb")) {
      try {
        GlobalSessionPool.getInstance().executeNonQueryStatement("create database root.toyotads;");
        GlobalSessionPool.getInstance()
            .executeNonQueryStatement(
                "create device template t1 (AccelerationFB DOUBLE encoding=GORILLA, AccelerationLR DOUBLE encoding=GORILLA, Speed_TypeA DOUBLE encoding=GORILLA, SteeringAngle_TypeA DOUBLE encoding=GORILLA, EngineRPM_TypeA DOUBLE encoding=GORILLA, TirePressureFL_kpa DOUBLE encoding=GORILLA, latitude DOUBLE encoding=GORILLA, longitude DOUBLE encoding=GORILLA, AccelPedalAngle_TypeA DOUBLE encoding=GORILLA, TirePressureFR_kpa DOUBLE encoding=GORILLA, TirePressureRL_kpa DOUBLE encoding=GORILLA, TirePressureRR_kpa DOUBLE encoding=GORILLA, AmbientTemperature DOUBLE encoding=GORILLA, TemperatureD DOUBLE encoding=GORILLA, FuelGageIndication INT32 encoding=TS_2DIFF, TurnLampSwitchStatus INT32 encoding=TS_2DIFF, ATShiftPosition INT32 encoding=TS_2DIFF, BrakePedal INT32 encoding=TS_2DIFF, DoorOpenD INT32 encoding=TS_2DIFF, ParkingBrake INT32 encoding=TS_2DIFF, EcoModeIndicator INT32 encoding=TS_2DIFF, PowerModeSelect_TypeA INT32 encoding=TS_2DIFF, SportModeSelect INT32 encoding=TS_2DIFF, WindowPositionD INT32 encoding=TS_2DIFF, AirConIndicator INT32 encoding=TS_2DIFF, Odometer_km INT32 encoding=TS_2DIFF, HeadLamp_TypeB INT32 encoding=TS_2DIFF);");
        GlobalSessionPool.getInstance()
            .executeNonQueryStatement("set device template t1 to root.toyotads;");
        logger.info("Set schema template successfully");
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        // ignore
      }
    }
  }

  private static void startLoadingCSV() {
    setSchemaTemplate();

    int loadingThreadCount = csvDirectories.size();
    logger.info("Using {} threads to load CSV", loadingThreadCount);
    writeService =
        Executors.newFixedThreadPool(
            loadingThreadCount,
            new ThreadFactory() {
              private final AtomicInteger counter = new AtomicInteger(0);

              @Override
              public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("CSVLoad-" + counter.incrementAndGet());
                return thread;
              }
            });
    for (Path csvDirectory : csvDirectories) {
      writeService.submit(new CSVLoader(csvDirectory));
    }
  }

  private static void startGenerating() {
    // compute generating thread
    int generatingThreadCount = 0;
    for (List<Device> devices : intervalDeviceMap.values()) {
      generatingThreadCount += devices.size() / 2000 + ((devices.size() % 2000) > 0 ? 1 : 0);
    }
    logger.info("Using {} threads to generate workload", generatingThreadCount);
    writeService =
        Executors.newFixedThreadPool(
            generatingThreadCount,
            new ThreadFactory() {
              private final AtomicInteger counter = new AtomicInteger(0);

              @Override
              public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("DataGeneration-" + counter.incrementAndGet());
                return thread;
              }
            });
    double idealPtsPerSec = 0;
    for (Map.Entry<Long, List<Device>> entry : intervalDeviceMap.entrySet()) {
      long interval = entry.getKey();
      List<Device> devices = entry.getValue();
      for (int i = 0; i < devices.size(); i += 2000) {
        int end = Math.min(i + 2000, devices.size());
        writeService.submit(new DataGenerator(interval, devices.subList(i, end)));
      }
      AtomicLong measurementsCount = new AtomicLong(0);
      devices.forEach(x -> measurementsCount.addAndGet(x.getMeasurementCount()));
      idealPtsPerSec += measurementsCount.doubleValue() * entry.getValue().get(0).getFreq();
    }
    logger.info("Ideal points per second: {}", (long) idealPtsPerSec);
    // help for gc
    intervalDeviceMap = null;
  }

  private static void waitEnding() throws InterruptedException {
    while (true) {
      if (Configuration.shouldEnd()) {
        consumerService.shutdownNow();
        writeService.shutdownNow();
        System.exit(-1);
        break;
      } else {
        Thread.sleep(10_000);
      }
    }
  }
}
