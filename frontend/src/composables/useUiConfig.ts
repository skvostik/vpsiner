import { onMounted, ref } from 'vue'
import { api } from '../api'
import type { CustomLink } from '../types'

export const appName = ref('VPSiner')
export const appEyebrow = ref('Simply Observed')
export const customLinks = ref<CustomLink[]>([
  {
    icon: 'Github',
    label: 'GitHub',
    url: 'https://github.com/skvostik/vpsiner',
  },
])
let loaded = false

export async function fetchUiConfig() {
  try {
    const config = await api.config.ui()
    if (typeof config?.name === 'string') {
      appName.value = config.name
    }
    if (typeof config?.eyebrow === 'string') {
      appEyebrow.value = config.eyebrow
    }
    if (config?.links && Array.isArray(config.links)) {
      customLinks.value = config.links
    } else if (config && 'links' in config && config.links === undefined) {
      customLinks.value = []
    }
    loaded = true
  } catch {
    // Keep defaults if failed
  }
}

export function useUiConfig() {
  onMounted(() => {
    if (!loaded) {
      fetchUiConfig()
    }
  })

  return {
    appName,
    appEyebrow,
    customLinks,
    fetchUiConfig,
  }
}
