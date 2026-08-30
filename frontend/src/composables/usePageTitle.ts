import { ref, toValue, watchEffect, type MaybeRefOrGetter } from 'vue'
import { appEyebrow, appName } from './useUiConfig'

/** Header title, rendered by AppHeader and set by the active view through usePageTitle(). */
export const pageTitle = ref('')

watchEffect(() => {
  const current = pageTitle.value
  const name = appName.value || 'VPSiner'
  const eyebrow = appEyebrow.value
  if (current && current !== name) {
    document.title = `${current} · ${name}`
  } else if (eyebrow) {
    document.title = `${name} - ${eyebrow}`
  } else {
    document.title = name
  }
})

export function usePageTitle(source: MaybeRefOrGetter<string | undefined>) {
  watchEffect(() => {
    pageTitle.value = toValue(source) || ''
  })
}
