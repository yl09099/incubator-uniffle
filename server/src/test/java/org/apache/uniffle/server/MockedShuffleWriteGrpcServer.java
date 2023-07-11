package org.apache.uniffle.server;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.metrics.GRPCMetrics;
import org.apache.uniffle.common.rpc.GrpcServer;

public class MockedShuffleWriteGrpcServer extends GrpcServer {
  MockedShuffleServerWriteGrpcService service;

  public MockedShuffleWriteGrpcServer(RssBaseConf conf, MockedShuffleServerWriteGrpcService service,
                          GRPCMetrics grpcMetrics) {
    super(conf, Lists.newArrayList(Pair.of(service, Lists.newArrayList())), grpcMetrics);
    this.service = service;
  }

  public MockedShuffleServerWriteGrpcService getService() {
    return service;
  }
}
