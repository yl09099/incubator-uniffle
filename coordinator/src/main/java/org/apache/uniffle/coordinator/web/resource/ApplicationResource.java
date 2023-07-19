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

package org.apache.uniffle.coordinator.web.resource;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletContext;

import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

import org.apache.uniffle.coordinator.ApplicationManager;
import org.apache.uniffle.coordinator.web.Response;
import org.apache.uniffle.coordinator.web.vo.AppInfoVO;
import org.apache.uniffle.coordinator.web.vo.UserAppNumVO;

@Produces({ MediaType.APPLICATION_JSON })
public class ApplicationResource extends BaseResource {

  @Context
  protected ServletContext servletContext;

  @GET
  @Path("/total")
  public Response getAppTotality() {
    return execute(() -> {
      Set<String> appIds = getApplicationManager().getAppIds();
      Map<String, Integer> appTotalityMap = new HashMap<>();
      appTotalityMap.put("appTotality", appIds.size());
      return appTotalityMap;
    });
  }

  @GET
  @Path("/usertotal")
  public Response<List> getUserApps() {
    return execute(() -> {
      Map<String, Map<String, Long>> currentUserAndApp = getApplicationManager()
          .getQuotaManager().getCurrentUserAndApp();
      List<UserAppNumVO> usercnt = new ArrayList<>();
      for (Map.Entry<String, Map<String, Long>> stringMapEntry : currentUserAndApp.entrySet()) {
        String userName = stringMapEntry.getKey();
        usercnt.add(new UserAppNumVO(userName, stringMapEntry.getValue().size()));
      }
      usercnt.sort((UserAppNumVO o1, UserAppNumVO o2) -> o1.getAppNum() - o2.getAppNum());
      return usercnt;
    });
  }

  @GET
  @Path("appinfos")
  public Response<List<AppInfoVO>> getAppInfoList() {
    return execute(() -> {
      List<AppInfoVO> userToAppList = new ArrayList<>();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Map<String, Map<String, Long>> currentUserAndApp = getApplicationManager()
          .getQuotaManager().getCurrentUserAndApp();
      for (Map.Entry<String, Map<String, Long>> userAppIdTimestampMap : currentUserAndApp.entrySet()) {
        String userName = userAppIdTimestampMap.getKey();
        for (Map.Entry<String, Long> appIdTimestampMap : userAppIdTimestampMap.getValue().entrySet()) {
          userToAppList.add(new AppInfoVO(userName,appIdTimestampMap.getKey(),
              sdf.format(new Date(appIdTimestampMap.getValue()))));
        }
      }
      return userToAppList;
    });
  }

  private ApplicationManager getApplicationManager() {
    return (ApplicationManager) servletContext.getAttribute(
        ApplicationManager.class.getCanonicalName());
  }
}
