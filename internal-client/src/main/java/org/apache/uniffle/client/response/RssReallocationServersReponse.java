package org.apache.uniffle.client.response;

import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.proto.RssProtos;

public class RssReallocationServersReponse extends ClientResponse {

  private boolean reallocationFlag;

  public RssReallocationServersReponse(StatusCode statusCode, String message, boolean reallocationFlag) {
    super(statusCode, message);
    this.reallocationFlag = reallocationFlag;
  }

  public boolean isReallocationFlag() {
    return reallocationFlag;
  }

  public static RssReallocationServersReponse fromProto(RssProtos.ReallocationServersReponse response) {
    return new RssReallocationServersReponse(
        // todo: [issue#780] add fromProto for StatusCode issue
        StatusCode.valueOf(response.getStatus().name()),
        response.getMsg(),
        response.getReallocationFlag()
    );
  }
}
