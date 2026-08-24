<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import {
  darkTheme,
  NConfigProvider,
  NButton,
  NGlobalStyle,
  NLayout,
  NLayoutContent,
  NMessageProvider,
  type GlobalThemeOverrides,
} from 'naive-ui'
import { RouterView } from 'vue-router'
import { Menu } from '@lucide/vue'
import AppHeader from './components/AppHeader.vue'
import AppSidebar from './components/AppSidebar.vue'
import BackendOfflineScreen from './components/BackendOfflineScreen.vue'
import { useBackendHealth } from './composables/useBackendHealth'

const isDark = ref(false)
const isMobile = ref(false)
const sidebarCollapsed = ref(false)
const { backendOnline } = useBackendHealth()
let colorSchemeQuery: MediaQueryList | undefined
let mobileQuery: MediaQueryList | undefined

const currentTheme = computed(() => (isDark.value ? darkTheme : null))
const themeOverrides: GlobalThemeOverrides = {
  common: {
    primaryColor: '#0891b2',
    primaryColorHover: '#06b6d4',
    primaryColorPressed: '#0e7490',
    borderRadius: '4px',
  },
}
function updateTheme(event: MediaQueryListEvent | MediaQueryList) {
  isDark.value = event.matches
}
function updateMobile(event: MediaQueryListEvent | MediaQueryList) {
  isMobile.value = event.matches
  sidebarCollapsed.value = event.matches
}

onMounted(() => {
  colorSchemeQuery = window.matchMedia('(prefers-color-scheme: dark)')
  updateTheme(colorSchemeQuery)
  colorSchemeQuery.addEventListener('change', updateTheme)

  mobileQuery = window.matchMedia('(max-width: 768px)')
  updateMobile(mobileQuery)
  mobileQuery.addEventListener('change', updateMobile)
})

onBeforeUnmount(() => {
  colorSchemeQuery?.removeEventListener('change', updateTheme)
  mobileQuery?.removeEventListener('change', updateMobile)
})
</script>

<template>
  <n-config-provider :theme="currentTheme" :theme-overrides="themeOverrides">
    <n-global-style />
    <n-message-provider placement="bottom">
      <BackendOfflineScreen v-if="!backendOnline" />
      <n-layout v-else has-sider class="h-screen">
        <AppSidebar v-model:collapsed="sidebarCollapsed" :mobile="isMobile" />
        <n-layout-content>
          <AppHeader>
            <template #leading>
              <n-button
                v-if="isMobile"
                circle
                secondary
                aria-label="Toggle menu"
                @click="sidebarCollapsed = !sidebarCollapsed"
              >
                <template #icon>
                  <Menu :size="18" />
                </template>
              </n-button>
            </template>
          </AppHeader>
          <div class="mx-auto max-w-7xl px-5 py-6 sm:px-8 sm:py-8">
            <router-view />
          </div>
        </n-layout-content>
      </n-layout>
    </n-message-provider>
  </n-config-provider>
</template>
