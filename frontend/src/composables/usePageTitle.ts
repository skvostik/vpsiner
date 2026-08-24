import { ref, toValue, watchEffect, type MaybeRefOrGetter } from 'vue'

const fallbackTitle = 'VPSiner'

/** Header title, rendered by AppHeader and set by the active view through usePageTitle(). */
export const pageTitle = ref(fallbackTitle)

export function usePageTitle(source: MaybeRefOrGetter<string | undefined>) {
  watchEffect(() => {
    pageTitle.value = toValue(source) || fallbackTitle
  })
}
