<template>
  <div class="demo-collapse">
    <el-collapse v-model="pageData.activeNames" accordion:false>
      <el-collapse-item title="Coordinator Server" name="1">
        <div>
          <el-descriptions
              class="margin-top"
              :column="3"
              :size="size"
              border>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Platform/>
                  </el-icon>
                  Coordinatro ID
                </div>
              </template>
              {{ pageData.serverInfo.coordinatorId }}
            </el-descriptions-item>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Link/>
                  </el-icon>
                  IP Address
                </div>
              </template>
              {{ pageData.serverInfo.serverIp }}
            </el-descriptions-item>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Wallet/>
                  </el-icon>
                  Coordinator Server Port
                </div>
              </template>
              {{ pageData.serverInfo.serverPort }}
            </el-descriptions-item>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Wallet/>
                  </el-icon>
                  Coordinator Web Port
                </div>
              </template>
              {{ pageData.serverInfo.serverWebPort }}
            </el-descriptions-item>
          </el-descriptions>
        </div>
      </el-collapse-item>
      <el-collapse-item title="Coordinator Properties" name="2">
        <el-table :data="pageData.tableData" style="width: 100%">
          <el-table-column prop="argumentKey" label="Name" min-width="380"/>
          <el-table-column prop="argumentValue" label="Value" min-width="380"/>
        </el-table>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>

<script>
import {ref, reactive, computed, onMounted} from 'vue'
import {getCoordinatorConf, getCoordinatorServerInfo} from "@/api/api";

export default {
  setup() {
    const pageData = reactive({
          activeNames: ['1', '2'],
          tableData: [],
          serverInfo: {}
        }
    );
    async function getCoordinatorServerConfPage() {
      const res = await getCoordinatorConf();
      pageData.serverInfo = res.data.data
    }
    async function getCoorServerInfo() {
      const res = await getCoordinatorServerInfo();
      pageData.tableData = res.data.data
    }
    onMounted(() => {
      getCoordinatorServerConfPage();
      getCoorServerInfo();
    })
    
    const size = ref('')
    const iconStyle = computed(() => {
      const marginMap = {
        large: '8px',
        default: '6px',
        small: '4px',
      }
      return {
        marginRight: marginMap[size.value] || marginMap.default,
      }
    })
    const blockMargin = computed(() => {
      const marginMap = {
        large: '32px',
        default: '28px',
        small: '24px',
      }
      return {
        marginTop: marginMap[size.value] || marginMap.default,
      }
    })
    return {
      pageData,
      iconStyle,
      blockMargin,
      size
    }
  }
}
</script>
<style>
.cell-item {
  display: flex;
  align-items: center;
}

.margin-top {
  margin-top: 20px;
}
</style>
