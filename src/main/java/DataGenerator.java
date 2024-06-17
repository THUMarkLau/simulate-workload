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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DataGenerator implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(DataGenerator.class);
  private final long interval;
  private final List<Device> devices;
  private long lastLogTime = 0L;

  public DataGenerator(long interval, List<Device> devices) {
    this.interval = interval;
    this.devices = devices;
  }

  @Override
  public void run() {
    while (true) {
      long currentTime = System.currentTimeMillis();
      DataQueue queue = DataQueue.getInstance();
      for (Device device : devices) {
        // generate data
        if (!queue.submit(device.generateData())
            && System.currentTimeMillis() - lastLogTime > 10_000) {
          logger.error("Failed to submit data for device {}", device.getDeviceId());
          lastLogTime = System.currentTimeMillis();
        }
      }
      long sleepTime = interval - (System.currentTimeMillis() - currentTime);
      if (sleepTime > 0) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
