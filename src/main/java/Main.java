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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);
  private static ExecutorService generationService;
  private static ExecutorService consumerService;
  private static Map<Long, List<Device>> intervalDeviceMap;

  public static void main(String[] args) throws Exception {
    loadConfig(args);
    initConsumerService();
    initSessionPool();
    startMonitor();
    registerSchema(intervalDeviceMap.values());
    startGenerating();
    waitEnding();
  }

  private static void loadConfig(String[] args) throws IOException {
    Configuration.parseConfig(args);
    PropertiesLoader propertiesLoader = new PropertiesLoader(new File(Configuration.configFile));
    intervalDeviceMap = propertiesLoader.load();
  }

  private static void initConsumerService() {
    // start threads for consuming
    consumerService = Executors.newFixedThreadPool(Configuration.clientCount);
    for (int i = 0; i < Configuration.clientCount; i++) {
      consumerService.submit(new DataConsumer());
    }
  }

  private static void initSessionPool() {
    GlobalSessionPool.getInstance().init();
    if (Configuration.clearBeforeStart) {
      try {
        GlobalSessionPool.getInstance().executeNonQueryStatement("delete database root.**");
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        // ignore
      }
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

  private static void startGenerating() {
    // compute generating thread
    int generatingThreadCount = 0;
    for (List<Device> devices : intervalDeviceMap.values()) {
      generatingThreadCount += devices.size() / 2000 + ((devices.size() % 2000) > 0 ? 1 : 0);
    }
    logger.info("Using {} threads to generate workload", generatingThreadCount);
    generationService = Executors.newFixedThreadPool(generatingThreadCount);
    double idealPtsPerSec = 0;
    for (Map.Entry<Long, List<Device>> entry : intervalDeviceMap.entrySet()) {
      long interval = entry.getKey();
      List<Device> devices = entry.getValue();
      for (int i = 0; i < devices.size(); i += 2000) {
        int end = Math.min(i + 2000, devices.size());
        generationService.submit(new DataGenerator(interval, devices.subList(i, end)));
      }
      idealPtsPerSec += devices.size() * 1000.0 / interval;
    }
    logger.info("Ideal points per second: {}", idealPtsPerSec);
    // help for gc
    intervalDeviceMap = null;
  }

  private static void waitEnding() throws InterruptedException {
    while (true) {
      if (Configuration.shouldEnd()) {
        consumerService.shutdownNow();
        generationService.shutdownNow();
        System.exit(-1);
        break;
      } else {
        Thread.sleep(10_000);
      }
    }
  }
}
