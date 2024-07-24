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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataQueue {
  private static final Logger logger = LoggerFactory.getLogger(DataQueue.class);
  private static final DataQueue instance = new DataQueue();
  private final BlockingQueue<Object> recordsQueue;

  private DataQueue() {
    recordsQueue = new ArrayBlockingQueue<>(Configuration.queueSize);
    logger.info("Queue size is {}", Configuration.queueSize);
  }

  public static DataQueue getInstance() {
    return instance;
  }

  public boolean submit(Object object) {
    return recordsQueue.offer(object);
  }

  public Object consume(long timeout) throws InterruptedException {
    return recordsQueue.poll(timeout, TimeUnit.MILLISECONDS);
  }

  public long size() {
    return recordsQueue.size();
  }
}
