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

package org.apache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uniffle.common.util.JavaUtils;

/**
 * We establish a Spark shuffleId and a stage attempt number, and the correspondence between them
 * and the Uniffle shuffleId. The Uniffle shuffleId is incrementing forever. For example:
 *
 * <p>| spark shuffleId | stage attemptnumber | uniffle shuffleId |
 *
 * <p>| 0 | 0 | 0 |
 *
 * <p>| 0 | 1 | 1 |
 *
 * <p>| 0 | 2 | 2 |
 *
 * <p>| 0 | 3 | 3 |
 *
 * <p>| 1 | 0 | 4 |
 *
 * <p>...
 */
public class ShuffleIdMappingManager {
  // Generate a new ShuffleID.
  private AtomicInteger shuffleIdGenerator;
  // appShuffleId -> stageAttemptNumber -> newShuffleId.
  private Map<Integer, Map<Integer, Integer>> shuffleIdMapping;
  // Map the relationship between shuffleId and Determinate.
  private Map<Integer, Boolean> shuffleDeterminateMap;

  public ShuffleIdMappingManager() {
    shuffleIdGenerator = new AtomicInteger(-1);
    shuffleIdMapping = JavaUtils.newConcurrentMap();
    shuffleDeterminateMap = JavaUtils.newConcurrentMap();
  }

  /**
   * Record the Determinate status of each shuffle.
   *
   * @param shuffleId
   * @param isDeterminate
   */
  public void recordShuffleIdDeterminate(int shuffleId, boolean isDeterminate) {
    shuffleDeterminateMap.put(shuffleId, isDeterminate);
  }

  public boolean getShuffleIdDeterminate(int shuffleId) {
    return shuffleDeterminateMap.get(shuffleId);
  }

  /**
   * Create the shuffleId of uniffle based on the ShuffleID of Spark. When registerShuffle is being
   * performed, the default number of attempts by our stage is 0.
   *
   * @param shuffleId
   * @return
   */
  public int createUniffleShuffleId(int shuffleId) {
    return shuffleIdMapping
        .computeIfAbsent(
            shuffleId,
            id -> {
              Map<Integer, Integer> stageNumberToNewShuffleIdMap = JavaUtils.newConcurrentMap();
              int newShuffleId = shuffleIdGenerator.incrementAndGet();
              stageNumberToNewShuffleIdMap.computeIfAbsent(0, stageNumber -> newShuffleId);
              return stageNumberToNewShuffleIdMap;
            })
        .get(0);
  }

  /**
   * A new shuffleId is generated based on the shuffleId of Spark and the stage Attempt number of
   * Spark, and it is generally called only when the stage retry is triggered.
   *
   * @param shuffleId
   * @param stageAttemptNumber
   * @return
   */
  public int createUniffleShuffleId(int shuffleId, int stageAttemptNumber) {
    int generatorShuffleId = shuffleIdGenerator.incrementAndGet();
    shuffleIdMapping.get(shuffleId).put(stageAttemptNumber, generatorShuffleId);
    return generatorShuffleId;
  }

  /**
   * Determine whether there is a record based on the passed-in Spark shuffleId and the number of
   * Spark Stage attempts.
   *
   * @param shuffleId
   * @param stageAttemptNumber
   * @return
   */
  public boolean hasUniffleShuffleIdByStageNumber(int shuffleId, int stageAttemptNumber) {
    return shuffleIdMapping.get(shuffleId).get(stageAttemptNumber) != null;
  }

  public int getUniffleShuffleIdByStageNumber(int shuffleId, int stageAttemptNumber) {
    return shuffleIdMapping.get(shuffleId).get(stageAttemptNumber);
  }

  /**
   * Based on the passed-in Spark shuffleId, obtain all the stage attemptNumber. Sort in reverse
   * according to the stage attemptNumber to obtain the current Uniffle shuffleId
   *
   * @param shuffleId
   * @return
   */
  public int getUniffleShuffleIdByMaxStageNumber(int shuffleId) {
    return shuffleIdMapping.get(shuffleId).entrySet().stream()
        .sorted(Map.Entry.<Integer, Integer>comparingByKey().reversed())
        .findFirst()
        .get()
        .getValue();
  }
}
