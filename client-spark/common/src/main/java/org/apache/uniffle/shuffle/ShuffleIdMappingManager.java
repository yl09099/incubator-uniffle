/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.shuffle;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uniffle.common.util.JavaUtils;

/**
 * We establish a Spark shuffleId and a stage attemptId attemptNumber, and the correspondence
 * between them and the Uniffle shuffleId. The Uniffle shuffleId is incrementing forever. For
 * example:
 *
 * <p>| spark shuffleId | shuffleId_stageId_attemptNumber | uniffle shuffleId |
 *
 * <p>| 0 | 0_0_0 | 0 |
 *
 * <p>| 0 | 0_0_1 | 1 |
 *
 * <p>| 0 | 0_0_2 | 2 |
 *
 * <p>| 0 | 0_0_3 | 3 |
 *
 * <p>| 1 | 1_1_0 | 4 |
 *
 * <p>| 1 | 1_1_1 | 5 |
 *
 * <p>...
 */
public class ShuffleIdMappingManager {
  // Generate a new ShuffleID.
  private AtomicInteger shuffleIdGenerator;
  // appShuffleId -> app_stageid_attemptnumber -> newShuffleId.
  private Map<Integer, Map<String, Integer>> shuffleIdMapping;
  // Map the relationship between shuffleId and Determinate.
  private Map<Integer, Boolean> shuffleDeterminateMap;

  public ShuffleIdMappingManager() {
    shuffleIdGenerator = new AtomicInteger(-1);
    shuffleIdMapping = JavaUtils.newConcurrentMap();
    shuffleDeterminateMap = JavaUtils.newConcurrentMap();
  }

  /**
   * Create the shuffleId of uniffle based on the ShuffleID of Spark.
   *
   * @param shuffleId
   * @return
   */
  public int createUniffleShuffleId(int shuffleId, String appShuffleIdentifier) {
    Map<String, Integer> appShuffleIdentifier2NewShuffleIdMap = shuffleIdMapping.get(shuffleId);
    if (appShuffleIdentifier2NewShuffleIdMap == null) {
      appShuffleIdentifier2NewShuffleIdMap = JavaUtils.newConcurrentMap();
      appShuffleIdentifier2NewShuffleIdMap.computeIfAbsent(
          appShuffleIdentifier, k -> shuffleIdGenerator.incrementAndGet());
      return appShuffleIdentifier2NewShuffleIdMap.get(appShuffleIdentifier);
    } else {
      return appShuffleIdentifier2NewShuffleIdMap.computeIfAbsent(
          appShuffleIdentifier, k -> shuffleIdGenerator.incrementAndGet());
    }
  }

  /**
   * Whether it has a uniffle shuffleId with a unique appShuffleIdentifier.
   *
   * @param shuffleId
   * @param appShuffleIdentifier
   * @return
   */
  public boolean hasUniffleShuffleId(int shuffleId, String appShuffleIdentifier) {
    if (shuffleIdMapping.isEmpty()
        || shuffleIdMapping.get(shuffleId) == null
        || shuffleIdMapping.get(shuffleId).get(appShuffleIdentifier) == null) {
      return false;
    }
    return true;
  }

  /**
   * Obtain the uniffle shuffleId based on the appShuffleIdentifier.
   *
   * @param shuffleId
   * @param appShuffleIdentifier
   * @return
   */
  public int getUniffleShuffleId(int shuffleId, String appShuffleIdentifier) {
    return shuffleIdMapping.get(shuffleId).get(appShuffleIdentifier);
  }

  /**
   * During the Shuffle Read stage, it is only necessary to obtain the maximum Uniffle ShuffleId.
   *
   * @param shuffleId
   * @return
   */
  public int getUniffleShuffleIdForRead(int shuffleId) {
    return shuffleIdMapping.get(shuffleId).values().stream()
        .sorted(Comparator.reverseOrder())
        .findFirst()
        .get();
  }

  public void recordShuffleIdDeterminate(int shuffleId, boolean isDeterminate) {
    shuffleDeterminateMap.put(shuffleId, isDeterminate);
  }

  public boolean getShuffleIdDeterminate(int shuffleId) {
    return shuffleDeterminateMap.get(shuffleId);
  }
}
