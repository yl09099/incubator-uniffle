<template>
  <div>
    <el-table :data="pageData.tableData" height="550" style="width: 100%">
      <el-table-column prop="excludeNodeId" label="ExcludeNodeId" min-width="180"/>
    </el-table>
  </div>
</template>
<script>
import {onMounted, reactive} from 'vue'
import { getShuffleExcludeNodes } from "@/api/api";

export default {
  setup() {
    const pageData = reactive({
      tableData: [
        {
          excludeNodeId:""
        }
      ]
    })
    async function getShuffleExcludeNodesPage() {
      const res = await getShuffleExcludeNodes();
      pageData.tableData = res.data.data
    }

    onMounted(() => {
      getShuffleExcludeNodesPage();
    })
    return {pageData}
  }
}
</script>