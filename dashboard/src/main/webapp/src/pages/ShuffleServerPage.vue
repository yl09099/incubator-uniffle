<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<template>
  <div class="page-wrap">
    <div class="stats-row">
      <router-link class="stat-link" :to="{ path: '/shuffleserverpage/activeNodeList', query: { isExcludedPage: false } }" @click.native="routerHandler">
        <div class="stat-card"><div class="stat-label">Active</div><div class="stat-value sv-success">{{ dataList.allshuffleServerSize.ACTIVE ?? 0 }}</div></div>
      </router-link>
      <router-link class="stat-link" :to="{ path: '/shuffleserverpage/decommissioningNodeList', query: { isExcludedPage: false } }" @click.native="routerHandler">
        <div class="stat-card"><div class="stat-label">Decommissioning</div><div class="stat-value sv-primary">{{ dataList.allshuffleServerSize.DECOMMISSIONING ?? 0 }}</div></div>
      </router-link>
      <router-link class="stat-link" :to="{ path: '/shuffleserverpage/decommissionedNodeList', query: { isExcludedPage: false } }" @click.native="routerHandler">
        <div class="stat-card"><div class="stat-label">Decommissioned</div><div class="stat-value sv-purple">{{ dataList.allshuffleServerSize.DECOMMISSIONED ?? 0 }}</div></div>
      </router-link>
      <router-link class="stat-link" :to="{ path: '/shuffleserverpage/lostNodeList', query: { isExcludedPage: false } }" @click.native="routerHandler" :updateTotalPage="updateTotalPage">
        <div class="stat-card"><div class="stat-label">Lost</div><div class="stat-value sv-danger">{{ dataList.allshuffleServerSize.LOST ?? 0 }}</div></div>
      </router-link>
      <router-link class="stat-link" :to="{ path: '/shuffleserverpage/unhealthyNodeList', query: { isExcludedPage: false } }" @click.native="routerHandler">
        <div class="stat-card"><div class="stat-label">Unhealthy</div><div class="stat-value sv-warning">{{ dataList.allshuffleServerSize.UNHEALTHY ?? 0 }}</div></div>
      </router-link>
      <router-link class="stat-link" :to="{ path: '/shuffleserverpage/excludeNodeList', query: { isExcludedPage: true } }" @click.native="routerHandler">
        <div class="stat-card"><div class="stat-label">Excludes</div><div class="stat-value sv-default">{{ dataList.allshuffleServerSize.EXCLUDED ?? 0 }}</div></div>
      </router-link>
    </div>
    <div class="table-section flex-fill">
      <router-view></router-view>
    </div>
  </div>
</template>

<script>
import { onMounted, reactive, provide } from 'vue'
import { getShufflegetStatusTotal } from '@/api/api'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'

export default {
  setup() {
    const dataList = reactive({ allshuffleServerSize: { ACTIVE: 0, DECOMMISSIONED: 0, DECOMMISSIONING: 0, EXCLUDED: 0, LOST: 0, UNHEALTHY: 0 } })
    const currentServerStore = useCurrentServerStore()

    async function getShuffleStatusTotalPage() {
      try { const res = await getShufflegetStatusTotal(); dataList.allshuffleServerSize = res?.data?.data || dataList.allshuffleServerSize } catch (e) { console.warn('Failed to fetch shuffle status total:', e) }
    }

    currentServerStore.$subscribe((mutable, state) => { if (state.currentServer) getShuffleStatusTotalPage() })
    onMounted(() => { if (currentServerStore.currentServer) getShuffleStatusTotalPage() })

    const routerHandler = () => { getShuffleStatusTotalPage() }
    provide('updateTotalPage', () => { getShuffleStatusTotalPage() })

    return { dataList, routerHandler }
  }
}
</script>

<style scoped>
.stat-link {
  text-decoration: none;
  flex: 1;
  display: block;
}
</style>
