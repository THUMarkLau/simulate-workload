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

import java.util.concurrent.atomic.AtomicInteger;

public class SchemaRegisterTask implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(SchemaRegisterTask.class);
  private final Device device;
  public static final AtomicInteger finishedCount = new AtomicInteger(0);
  public static final AtomicInteger totalCount = new AtomicInteger(0);

  public SchemaRegisterTask(Device device) {
    this.device = device;
  }

  @Override
  public void run() {
    try {
      device.createSchema();
      finishedCount.incrementAndGet();
    } catch (Exception e) {
      logger.error("Failed to create schema for device {}", device.getDeviceId(), e);
      Configuration.failed.set(true);
    }
  }
}
