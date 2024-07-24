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

import java.util.concurrent.atomic.AtomicBoolean;

public class Configuration {
  public static String configFile = "config.txt";
  public static String dbIp = "127.0.0.1";
  public static int requestSize = 100;
  public static double timeout = 10;
  public static int queueSize = 16 * 1024;
  public static long maxRunTime = Long.MAX_VALUE;
  public static int clientCount = 10;
  private static long startTime = System.currentTimeMillis();
  public static AtomicBoolean failed = new AtomicBoolean(false);
  public static String storageGroupName = "root.sg";
  public static boolean registerSchema = false;
  public static boolean registerSchemaOnly = false;
  public static boolean clearBeforeStart = false;
  public static boolean enableMonitor = false;
  public static boolean loadCSV = false;
  public static String mode = "iotdb";

  public static void parseConfig(String[] args) {
    for (int i = 0; i < args.length; ++i) {
      if (args[i].equals("--config")) {
        configFile = args[++i];
      } else if (args[i].equals("--ip")) {
        dbIp = args[++i];
      } else if (args[i].equals("--rsize")) {
        requestSize = Integer.parseInt(args[++i]);
      } else if (args[i].equals("--timeout")) {
        timeout = Double.parseDouble(args[++i]);
      } else if (args[i].equals("--max-time")) {
        maxRunTime = (long) (Double.parseDouble(args[++i]) * 1000L);
      } else if (args[i].equals("--queue-size")) {
        queueSize = Integer.parseInt(args[++i]);
      } else if (args[i].equals("--client-count")) {
        clientCount = Integer.parseInt(args[++i]);
      } else if (args[i].equals("--sg")) {
        storageGroupName = args[++i];
      } else if (args[i].equals("--clear")) {
        clearBeforeStart = true;
      } else if (args[i].equals("--monitor")) {
        enableMonitor = true;
      } else if (args[i].equals("--register-schema")) {
        registerSchema = true;
      } else if (args[i].equals("--register-schema-only")) {
        registerSchemaOnly = true;
      } else if (args[i].equals("--mode")) {
        mode = args[++i];
      } else if (args[i].equals("--load-csv")) {
        loadCSV = true;
      }
    }
  }

  public static boolean shouldEnd() {
    return System.currentTimeMillis() - startTime > maxRunTime || failed.get();
  }
}
