<template>
  <div class="common-layout">
    <el-container>
      <el-header>
        <el-row>
          <el-col :span="24">
            <el-menu
                :default-active="activeIndex1"
                class="el-menu-demo"
                mode="horizontal"
                background-color="#545c64"
                text-color="#fff"
                active-text-color="#ffd04b"
                @select="handleSelect">
              <el-menu-item index="0">
                <div class="unffilelogo">
                  <img src="../assets/uniffle-logo.png" alt="unffile">
                </div>
              </el-menu-item>
              <router-link to="/coordinatorserverpage">
                <el-menu-item index="1">
                  Coordinator Server
                </el-menu-item>
              </router-link>
              <router-link to="/shuffleserverpage">
                <el-menu-item index="2">
                  Shuffle Server
                </el-menu-item>
              </router-link>
              <router-link to="/applicationpage">
                <el-menu-item index="3">
                  Application
                </el-menu-item>
              </router-link>
            </el-menu>
          </el-col>
        </el-row>
      </el-header>
      <el-main>
        <router-view></router-view>
      </el-main>
    </el-container>
  </div>
</template>

<script>
import {ref, onMounted} from 'vue'

export default {
  setup() {
    const activeIndex1 = ref('1')

    function handleSelect(index, path) {
      console.log("index", index)
      console.log("path", path)
      localStorage.setItem("menuId", JSON.stringify(activeIndex1))
    }

    onMounted(() => {

    })
    return {activeIndex1, handleSelect}
  }
}
</script>

<style>
a {
  text-decoration: none;
  color: white;
}

.unffilelogo {
  background-color: #545c64;
  height: 100%;
  position: relative;
}

.unffilelogo > img {
  height: 55px;
}
</style>
