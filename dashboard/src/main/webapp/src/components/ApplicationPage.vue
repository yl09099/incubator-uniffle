<template>
  <div>
    <el-row :gutter="20">
      <el-col :span="4">
        <el-card class="box-card" shadow="hover">
          <template #header>
            <div class="card-header">
              <span class="cardtile">APPS TOTAL</span>
            </div>
          </template>
          <div class="appcnt">{{ pageData.apptotal.appTotality }}</div>
        </el-card>
      </el-col>
    </el-row>
    <el-divider/>
    <el-tag>User App ranking</el-tag>
    <div>
      <el-table :data="pageData.userAppCount" height="250" style="width: 100%">
        <el-table-column prop="userName" label="UserName" min-width="180"/>
        <el-table-column prop="appNum" label="Totality" min-width="180"/>
      </el-table>
    </div>
    <el-divider/>
    <el-tag>Apps</el-tag>
    <div>
      <el-table :data="pageData.appInfoData" height="250" style="width: 100%">
        <el-table-column prop="appId" label="AppId" min-width="180"/>
        <el-table-column prop="userName" label="UserName" min-width="180"/>
        <el-table-column prop="updateTime" label="Update Time" min-width="180"/>
      </el-table>
    </div>
  </div>
</template>

<script>
import {
  getApplicationInfoList,
  getAppTotal,
  getTotalForUser
} from "@/api/api";
import {onMounted, reactive} from "vue";

export default {
  setup() {
    const pageData = reactive({
      apptotal: {},
      userAppCount: [{}],
      appInfoData: [{appId: "", userName: "", updateTime: ""}]
    })

    async function getApplicationInfoListPage() {
      const res = await getApplicationInfoList();
      console.log(res)
      pageData.appInfoData = res.data.data
    }

    async function getTotalForUserPage() {
      const res = await getTotalForUser();
      console.log(res)
      pageData.userAppCount = res.data.data
    }

    async function getAppTotalPage() {
      const res = await getAppTotal();
      pageData.apptotal = res.data.data
    }

    onMounted(() => {
      getApplicationInfoListPage();
      getTotalForUserPage();
      getAppTotalPage();
    })
    return {pageData}
  }
}
</script>

<style>
.appcnt {
  font-family: "Lantinghei SC";
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
}
</style>
