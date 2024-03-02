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

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ValuePool {
  private static final ValuePool instance = new ValuePool();
  private boolean[] boolValues = new boolean[2048];
  private int[] intValues = new int[2048];
  private long[] longValues = new long[2048];
  private float[] floatValues = new float[2048];
  private double[] doubleValues = new double[2048];
  private String[] textValues = new String[2048];
  private AtomicInteger counter = new AtomicInteger(0);

  private ValuePool() {
    Random random = new Random();
    for (int i = 0; i < 2048; ++i) {
      boolValues[i] = random.nextBoolean();
      intValues[i] = random.nextInt();
      longValues[i] = random.nextLong();
      floatValues[i] = random.nextFloat();
      doubleValues[i] = random.nextDouble();
      textValues[i] = String.valueOf(random.nextLong());
    }
  }

  public static ValuePool getInstance() {
    return instance;
  }

  public boolean getBoolean() {
    return boolValues[counter.getAndIncrement() & 2047];
  }

  public int getInt() {
    return intValues[counter.getAndIncrement() & 2047];
  }

  public long getLong() {
    return longValues[counter.getAndIncrement() & 2047];
  }

  public float getFloat() {
    return floatValues[counter.getAndIncrement() & 2047];
  }

  public double getDouble() {
    return doubleValues[counter.getAndIncrement() & 2047];
  }

  public String getText() {
    return textValues[counter.getAndIncrement() & 2047];
  }
}
