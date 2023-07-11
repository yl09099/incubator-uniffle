package org.apache.uniffle.client.request;

import org.apache.uniffle.proto.RssProtos;

public class RssPartitionToShuffleServerRequest {
  private int shuffleId;

  public RssPartitionToShuffleServerRequest(int shuffleId) {
    this.shuffleId = shuffleId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public RssProtos.PartitionToShuffleServerRequest toProto() {
    RssProtos.PartitionToShuffleServerRequest.Builder builder = RssProtos.PartitionToShuffleServerRequest.newBuilder();
    builder.setShuffleId(shuffleId);
    return builder.build();
  }
}
