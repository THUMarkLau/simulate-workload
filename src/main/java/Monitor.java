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

public class Monitor implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(Monitor.class);

  @Override
  public void run() {
    while (SchemaRegisterTask.finishedCount.get() < SchemaRegisterTask.totalCount.get()) {
      logger.info(
          "Registering schema: {}/{}",
          SchemaRegisterTask.finishedCount.get(),
          SchemaRegisterTask.totalCount.get());
      try {
        Thread.sleep(10_000);
      } catch (Exception e) {
        logger.error("Meets error", e);
        return;
      }
    }

    long time = System.currentTimeMillis();
    while (true) {
      try {
        Thread.sleep(10_000);
      } catch (InterruptedException e) {
        // ignore
      }
      logger.info(
          "Current queue size is {}, average write speed is {} pts/seconds",
          DataQueue.getInstance().size(),
          (long)
              (((double) DataConsumer.pointsCounter.getAndSet(0))
                  * 1000L
                  / (System.currentTimeMillis() - time)));
    }
  }
}
