import { onMounted, ref } from 'vue'
import { api } from '../api'
import type { CustomLink } from '../types'

const customLinks = ref<CustomLink[]>([])
let loaded = false

export async function fetchUiConfig() {
  try {
    const config = await api.config.ui()
    if (config?.links && Array.isArray(config.links)) {
      customLinks.value = config.links
    } else {
      customLinks.value = []
    }
    loaded = true
  } catch {
    // Keep existing customLinks if any, or default
    if (!loaded) {
      customLinks.value = [
        {
          icon: 'Github',
          label: 'GitHub',
          url: 'https://github.com/skvostik/vpsiner',
        },
      ]
    }
  }
}

export function useUiConfig() {
  onMounted(() => {
    if (!loaded) {
      fetchUiConfig()
    }
  })

  return {
    customLinks,
    fetchUiConfig,
  }
}
