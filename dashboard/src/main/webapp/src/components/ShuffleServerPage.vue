<template>
  <div>
    <el-row :gutter="20">
      <el-col :span="4">
        <router-link to="/shuffleserverpage/activeNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Active</span>
              </div>
            </template>
            <div class="activenode">{{ dataList.allshuffleServerSize.activeNode }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/decommissioningNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Decommissioning</span>
              </div>
            </template>
            <div class="decommissioningnode">{{ dataList.allshuffleServerSize.decommissioningNode }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/decommissionedNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Decommissioned</span>
              </div>
            </template>
            <div class="decommissionednode">{{ dataList.allshuffleServerSize.decommissionedNode }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/lostNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Lost</span>
              </div>
            </template>
            <div class="lostnode">{{ dataList.allshuffleServerSize.lostNode }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/unhealthyNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Unhealthy</span>
              </div>
            </template>
            <div class="unhealthynode">{{ dataList.allshuffleServerSize.unhealthyNode }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/excludeNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Excludes</span>
              </div>
            </template>
            <div class="excludesnode">{{ dataList.allshuffleServerSize.excludesNode }}</div>
          </el-card>
        </router-link>
      </el-col>
    </el-row>
    <el-divider/>
    <el-row :gutter="24">
      <div style="width: 100%">
        <router-view></router-view>
      </div>
    </el-row>
  </div>
</template>

<script>
import {onMounted, reactive} from 'vue'
import {getShufflegetStatusTotal} from "@/api/api";

export default {
  setup() {
    const dataList = reactive({
      allshuffleServerSize: {
        activeNode: 0,
        decommissionedNode: 0,
        decommissioningNode: 0,
        excludesNode: 0,
        lostNode: 0,
        unhealthyNode: 0
      }
    })

    async function getShufflegetStatusTotalPage() {
      const res = await getShufflegetStatusTotal();
      dataList.allshuffleServerSize = res.data.data
    }

    onMounted(() => {
      getShufflegetStatusTotalPage();
    })
    return {dataList}
  }
}
</script>

<style scoped>
.cardtile {
  font-size: larger;
}

.activenode {
  font-family: "Lantinghei SC";
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: green;
}

.decommissioningnode {
  font-family: "Lantinghei SC";
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: #00c4ff;
}

.decommissionednode {
  font-family: "Lantinghei SC";
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: blue;
}

.lostnode {
  font-family: "Lantinghei SC";
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: red;
}

.unhealthynode {
  font-family: "Lantinghei SC";
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: #ff8800;
}

.excludesnode {
  font-family: "Lantinghei SC";
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
}
</style>