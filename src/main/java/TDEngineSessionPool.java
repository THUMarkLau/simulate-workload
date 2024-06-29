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

import com.taosdata.jdbc.TSDBDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

public class TDEngineSessionPool {
  private static final Logger logger = LoggerFactory.getLogger(TDEngineSessionPool.class);
  private static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();

  public static void init() {
    String jdbcUrl = "jdbc:TAOS://192.168.130.14:6030?user=root&password=taosdata";
    Properties connProps = new Properties();
    connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
    connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
    connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
    try {
      Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP DATABASE IF EXISTS MYTEST");
        stmt.execute("CREATE DATABASE IF NOT EXISTS MYTEST");
        stmt.execute("USE MYTEST");
      }
    } catch (Exception e) {
      e.printStackTrace();
      Configuration.failed.set(true);
    }
  }

  private static Connection getConn() {
    if (connectionThreadLocal.get() == null) {
      String jdbcUrl =
          String.format(
              "jdbc:TAOS://192.168.130.14:6030?user=root&password=taosdata", Configuration.dbIp);
      Properties connProps = new Properties();
      connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
      connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
      connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
      try {
        Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
        connectionThreadLocal.set(conn);
        try (Statement stmt = conn.createStatement()) {
          stmt.execute("USE MYTEST");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return connectionThreadLocal.get();
  }

  public static void sendRequest(List<Object> records) {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ");
    for (Object record : records) {
      sql.append(((Record) record).toTDengineSQL());
      DataConsumer.pointsCounter.addAndGet(((Record) record).measurements.size());
    }
    Connection conn = getConn();
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void createSchema(Device device) {
    Connection conn = getConn();
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(device.toTDengineSQL());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
