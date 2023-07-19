<template>
  <div>
    <el-table :data="pageData.tableData" height="550" style="width: 100%">
      <el-table-column prop="id" label="Id" min-width="180"/>
      <el-table-column prop="ip" label="IP" min-width="80"/>
      <el-table-column prop="grpcPort" label="Port" min-width="80"/>
      <el-table-column prop="usedMemory" label="UsedMem" min-width="80"/>
      <el-table-column prop="preAllocatedMemory" label="PreAllocatedMem" min-width="80"/>
      <el-table-column prop="availableMemory" label="AvailableMem" min-width="80"/>
      <el-table-column prop="totalMemory" label="TotalMem" min-width="80"/>
      <el-table-column prop="eventNumInFlush" label="FlushNum" min-width="80"/>
      <el-table-column prop="status" label="Status" min-width="80"/>
      <el-table-column prop="timestamp" label="ResigerTime" min-width="80"/>
      <el-table-column prop="tags" label="Tags" min-width="80"/>
    </el-table>
  </div>
</template>
<script>
import {onMounted, reactive} from 'vue'
import { getShuffleDecommissioningList } from "@/api/api";

export default {
  setup() {
    const pageData = reactive({
      tableData: [
        {
          id: "",
          ip: "",
          grpcPort: 0,
          usedMemory: 0,
          preAllocatedMemory: 0,
          availableMemory: 0,
          totalMemory: 0,
          eventNumInFlush: 0,
          tags: "",
          status: "",
          timestamp: ""
        }
      ]
    })

    async function getShuffleDecommissioningListPage() {
      const res = await getShuffleDecommissioningList();
      pageData.tableData = res.data.data
    }

    onMounted(() => {
      getShuffleDecommissioningListPage();
    })
    return {pageData}
  }
}
</script>