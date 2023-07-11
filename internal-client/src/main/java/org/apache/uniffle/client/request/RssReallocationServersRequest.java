package org.apache.uniffle.client.request;

import org.apache.uniffle.proto.RssProtos;

public class RssReallocationServersRequest {
  private int stageId;
  private int stageAttemptNumber;
  private int shuffleId;
  private int numPartitions;

  public RssReallocationServersRequest(int stageId, int stageAttemptNumber,int shuffleId,int numPartitions) {
    this.stageId = stageId;
    this.stageAttemptNumber = stageAttemptNumber;
    this.shuffleId = shuffleId;
    this.numPartitions = numPartitions;
  }

  public RssProtos.ReallocationServersRequest toProto() {
    RssProtos.ReallocationServersRequest.Builder builder = RssProtos.ReallocationServersRequest.newBuilder()
        .setStageId(stageId)
        .setStageAttemptNumber(stageAttemptNumber)
        .setShuffleId(shuffleId)
        .setNumPartitions(numPartitions);
    return builder.build();
  }
}
