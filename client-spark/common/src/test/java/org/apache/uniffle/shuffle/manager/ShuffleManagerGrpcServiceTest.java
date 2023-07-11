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

package org.apache.uniffle.shuffle.manager;

import java.util.List;

import com.clearspring.analytics.util.Lists;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.uniffle.client.request.RssReportShuffleWriteFailureRequest;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureResponse;
import org.apache.uniffle.proto.RssProtos.StatusCode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ShuffleManagerGrpcServiceTest {
  // create mock of RssShuffleManagerInterface.
  private static RssShuffleManagerInterface mockShuffleManager;
  private static final String appId = "app-123";
  private static final int maxFetchFailures = 2;
  private static final int shuffleId = 0;
  private static final int numMaps = 100;
  private static final int numReduces = 10;

  private static class MockedStreamObserver<T> implements StreamObserver<T> {
    T value;
    Throwable error;
    boolean completed;

    @Override
    public void onNext(T value) {
      this.value = value;
    }

    @Override
    public void onError(Throwable t) {
      this.error = t;
    }

    @Override
    public void onCompleted() {
      this.completed = true;
    }
  }

  @BeforeAll
  public static void setup() {
    mockShuffleManager = mock(RssShuffleManagerInterface.class);
    Mockito.when(mockShuffleManager.getAppId()).thenReturn(appId);
    Mockito.when(mockShuffleManager.getNumMaps(shuffleId)).thenReturn(numMaps);
    Mockito.when(mockShuffleManager.getPartitionNum(shuffleId)).thenReturn(numReduces);
    Mockito.when(mockShuffleManager.getMaxFetchFailures()).thenReturn(maxFetchFailures);
  }

  @Test
  public void testShuffleManagerGrpcService() {
    ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(mockShuffleManager);
    MockedStreamObserver<ReportShuffleFetchFailureResponse> appIdResponseObserver =
        new MockedStreamObserver<>();
    ReportShuffleFetchFailureRequest req =
        ReportShuffleFetchFailureRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptId(1)
            .setPartitionId(1)
            .buildPartial();

    service.reportShuffleFetchFailure(req, appIdResponseObserver);
    assertTrue(appIdResponseObserver.completed);
    // the first call of ReportShuffleFetchFailureRequest should be successful.
    assertEquals(StatusCode.SUCCESS, appIdResponseObserver.value.getStatus());
    assertFalse(appIdResponseObserver.value.getReSubmitWholeStage());

    // req with wrong appId should fail.
    req = ReportShuffleFetchFailureRequest.newBuilder().mergeFrom(req)
              .setAppId("wrong-app-id").build();
    service.reportShuffleFetchFailure(req, appIdResponseObserver);
    assertEquals(StatusCode.INVALID_REQUEST, appIdResponseObserver.value.getStatus());
    // forwards the stageAttemptId to 1 to mock invalid request
    req = ReportShuffleFetchFailureRequest.newBuilder()
              .mergeFrom(req)
              .setAppId(appId)
              .setStageAttemptId(0)
              .build();
    service.reportShuffleFetchFailure(req, appIdResponseObserver);
    assertEquals(StatusCode.INVALID_REQUEST, appIdResponseObserver.value.getStatus());
    assertTrue(appIdResponseObserver.value.getMsg().contains("old stage"));
  }

  @Test
  public void testReportShuffleWriteFailure() {
    List<ShuffleServerInfo> shuffleServers = Lists.newArrayList();
    ShuffleServerInfo shuffleServerInfo1 = new ShuffleServerInfo("server01", 0);
    ShuffleServerInfo shuffleServerInfo2 = new ShuffleServerInfo("server02", 0);
    ShuffleServerInfo shuffleServerInfo3 = new ShuffleServerInfo("server03", 0);
    ShuffleServerInfo shuffleServerInfo4 = new ShuffleServerInfo("server04", 0);
    shuffleServers.add(shuffleServerInfo1);
    shuffleServers.add(shuffleServerInfo2);
    shuffleServers.add(shuffleServerInfo3);
    shuffleServers.add(shuffleServerInfo4);
    /**
     * Determine whether the Application is the same
     */
    RssReportShuffleWriteFailureRequest rssFailureRequest =
        new RssReportShuffleWriteFailureRequest("appid01", 0, 0, shuffleServers, "");
    RssProtos.ReportShuffleWriteFailureRequest reportFailureRequest = rssFailureRequest.toProto();
    final ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(mockShuffleManager);
    final MockedStreamObserver<RssProtos.ReportShuffleWriteFailureResponse> appIdResponseObserver =
        new MockedStreamObserver<>();
    service.reportShuffleWriteFailure(reportFailureRequest, appIdResponseObserver);
    assertTrue(appIdResponseObserver.completed);
    assertEquals(StatusCode.INVALID_REQUEST, appIdResponseObserver.value.getStatus());
    assertTrue(appIdResponseObserver.value.getMsg().contains("wrong shuffle fetch failure report from appId"));

    /**
     * Normal submission application
     */
    RssReportShuffleWriteFailureRequest rssSuccessRequest =
        new RssReportShuffleWriteFailureRequest("app-123", 0, 0, shuffleServers, "");
    RssProtos.ReportShuffleWriteFailureRequest reprotSuccessRequest = rssSuccessRequest.toProto();
    service.reportShuffleWriteFailure(reprotSuccessRequest, appIdResponseObserver);
    assertTrue(appIdResponseObserver.completed);
    assertEquals(StatusCode.SUCCESS, appIdResponseObserver.value.getStatus());
    assertFalse(appIdResponseObserver.value.getReSubmitWholeStage());

    /**
     * Continuous commit to get the ShuffleServer list
     */
    RssReportShuffleWriteFailureRequest rssSuccessRequest01 =
        new RssReportShuffleWriteFailureRequest("app-123", 0, 0, shuffleServers, "");
    RssProtos.ReportShuffleWriteFailureRequest reprotSuccessRequest01 = rssSuccessRequest01.toProto();
    service.reportShuffleWriteFailure(reprotSuccessRequest01, appIdResponseObserver);

    List<ShuffleServerInfo> shuffleServers02 = Lists.newArrayList();
    shuffleServers02.add(shuffleServerInfo1);
    shuffleServers02.add(shuffleServerInfo2);
    shuffleServers02.add(shuffleServerInfo3);
    RssReportShuffleWriteFailureRequest rssSuccessRequest02 =
        new RssReportShuffleWriteFailureRequest("app-123", 0, 0, shuffleServers, "");
    RssProtos.ReportShuffleWriteFailureRequest reprotSuccessRequest02 = rssSuccessRequest02.toProto();
    service.reportShuffleWriteFailure(reprotSuccessRequest02, appIdResponseObserver);

    List<ShuffleServerInfo> shuffleServers03 = Lists.newArrayList();
    shuffleServers03.add(shuffleServerInfo1);
    shuffleServers03.add(shuffleServerInfo2);
    RssReportShuffleWriteFailureRequest rssSuccessRequest03 =
        new RssReportShuffleWriteFailureRequest("app-123", 0, 0, shuffleServers, "");
    RssProtos.ReportShuffleWriteFailureRequest reprotSuccessRequest03 = rssSuccessRequest03.toProto();
    service.reportShuffleWriteFailure(reprotSuccessRequest03, appIdResponseObserver);

    List<ShuffleServerInfo> shuffleServers04 = Lists.newArrayList();
    shuffleServers04.add(shuffleServerInfo1);
    shuffleServers04.add(shuffleServerInfo2);
    RssReportShuffleWriteFailureRequest rssSuccessRequest04 =
        new RssReportShuffleWriteFailureRequest("app-123", 0, 0, shuffleServers, "");
    RssProtos.ReportShuffleWriteFailureRequest reprotSuccessRequest04 = rssSuccessRequest04.toProto();
    service.reportShuffleWriteFailure(reprotSuccessRequest04, appIdResponseObserver);
    assertTrue(appIdResponseObserver.completed);
    assertEquals(StatusCode.SUCCESS, appIdResponseObserver.value.getStatus());
    assertTrue(appIdResponseObserver.value.getReSubmitWholeStage());
    assertTrue(appIdResponseObserver.value.getMsg().contains("write failure as maximum number(2)"));
  }
}
