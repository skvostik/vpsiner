<script setup lang="ts">
import { NDrawer, NDrawerContent, NLayoutSider } from 'naive-ui'
import AppSidebarMenu from './AppSidebarMenu.vue'

const props = defineProps<{
  collapsed: boolean
  mobile: boolean
}>()
const emit = defineEmits<{ 'update:collapsed': [value: boolean] }>()

function handleDrawerShow(show: boolean) {
  if (!show) emit('update:collapsed', true)
}
</script>

<template>
  <n-layout-sider
    v-if="!props.mobile"
    bordered
    collapse-mode="width"
    :collapsed-width="0"
    :width="250"
    :collapsed="props.collapsed"
    :native-scrollbar="false"
    show-trigger
    @collapse="emit('update:collapsed', true)"
    @expand="emit('update:collapsed', false)"
  >
    <AppSidebarMenu />
  </n-layout-sider>
  <n-drawer
    v-else
    :show="!props.collapsed"
    placement="left"
    :width="250"
    @update:show="handleDrawerShow"
  >
    <n-drawer-content :native-scrollbar="false">
      <AppSidebarMenu @navigate="emit('update:collapsed', true)" />
    </n-drawer-content>
  </n-drawer>
</template>
