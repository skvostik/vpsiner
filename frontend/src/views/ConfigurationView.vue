<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { NAlert, NSpin, NTag } from 'naive-ui'

import { api } from '../api'
import { usePageTitle } from '../composables/usePageTitle'
import type { ComputedEntry, SettingCategory, SettingEntry } from '../types'

usePageTitle('Configuration')

const settings = ref<SettingEntry[]>([])
const computedValues = ref<ComputedEntry[]>([])
const loading = ref(true)
const error = ref('')

const sections: { category: SettingCategory; title: string; hint: string }[] = [
  { category: 'common', title: 'Common', hint: 'Settings most deployments will want to review' },
  { category: 'advanced', title: 'Advanced', hint: 'Tuning knobs that rarely need changing' },
]

const byCategory = computed(() =>
  sections
    .map((section) => ({
      ...section,
      entries: settings.value
        .filter((entry) => entry.category === section.category)
        .sort((left, right) => left.name.localeCompare(right.name)),
    }))
    .filter((section) => section.entries.length > 0)
)

onMounted(async () => {
  try {
    const [loadedSettings, loadedComputedValues] = await Promise.all([
      api.config.settings(),
      api.config.computed(),
    ])
    settings.value = loadedSettings
    computedValues.value = loadedComputedValues
  } catch (fetchError) {
    error.value = fetchError instanceof Error ? fetchError.message : String(fetchError)
  } finally {
    loading.value = false
  }
})
</script>

<template>
  <div class="space-y-6">
    <p class="text-sm text-neutral-500 dark:text-neutral-400">
      Read-only view of the environment variables VPSiner supports and the values this instance is
      running with. Restart the backend with a different environment to change them.
    </p>

    <n-spin v-if="loading" size="small" />
    <n-alert v-else-if="error" type="error" title="Could not load configuration">
      {{ error }}
    </n-alert>
    <template v-else>
      <section v-if="computedValues.length" class="space-y-2">
        <div>
          <h2 class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">Computed</h2>
          <p class="text-xs text-neutral-500 dark:text-neutral-400">
            Values measured from the currently running backend
          </p>
        </div>
        <div class="overflow-x-auto rounded border border-neutral-200 dark:border-neutral-800">
          <table class="w-full border-collapse text-left text-sm">
            <thead
              class="bg-neutral-50 text-xs uppercase tracking-wide text-neutral-500 dark:bg-neutral-900 dark:text-neutral-400"
            >
              <tr>
                <th class="px-4 py-2 font-medium">Value</th>
                <th class="px-4 py-2 font-medium text-right">Current</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-neutral-200 dark:divide-neutral-800">
              <tr v-for="entry in computedValues" :key="entry.name">
                <td class="px-4 py-3 align-top">
                  <span class="font-mono text-xs text-neutral-900 dark:text-neutral-100">{{
                    entry.name
                  }}</span>
                  <p class="mt-1 text-xs text-neutral-500 dark:text-neutral-400">
                    {{ entry.description }}
                  </p>
                </td>
                <td class="px-4 py-3 align-top text-right">
                  <span
                    class="break-all font-mono text-xs text-neutral-900 dark:text-neutral-100"
                    >{{ entry.value }}</span
                  >
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
      <section v-for="section in byCategory" :key="section.category" class="space-y-2">
        <div>
          <h2 class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">
            {{ section.title }}
          </h2>
          <p class="text-xs text-neutral-500 dark:text-neutral-400">{{ section.hint }}</p>
        </div>
        <div class="overflow-x-auto rounded border border-neutral-200 dark:border-neutral-800">
          <table class="w-full border-collapse text-left text-sm">
            <thead
              class="bg-neutral-50 text-xs uppercase tracking-wide text-neutral-500 dark:bg-neutral-900 dark:text-neutral-400"
            >
              <tr>
                <th class="px-4 py-2 font-medium">Setting</th>
                <th class="px-4 py-2 font-medium text-right">Value</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-neutral-200 dark:divide-neutral-800">
              <tr v-for="entry in section.entries" :key="entry.name">
                <td class="px-4 py-3 align-top">
                  <div class="flex flex-wrap items-center gap-2">
                    <span class="font-mono text-xs text-neutral-900 dark:text-neutral-100">{{
                      entry.name
                    }}</span>
                    <n-tag v-if="entry.overridden" size="small" type="info" :bordered="false">
                      overridden
                    </n-tag>
                  </div>
                  <p class="mt-1 text-xs text-neutral-500 dark:text-neutral-400">
                    {{ entry.description }}
                  </p>
                </td>
                <td class="px-4 py-3 align-top text-right">
                  <span
                    v-if="entry.value"
                    class="break-all font-mono text-xs text-neutral-900 dark:text-neutral-100"
                    >{{ entry.value }}</span
                  >
                  <span v-else class="text-xs italic text-neutral-400 dark:text-neutral-500"
                    >not set</span
                  >
                  <p
                    v-if="entry.value !== entry.default"
                    class="mt-1 text-xs text-neutral-500 dark:text-neutral-400"
                  >
                    default:
                    <span v-if="entry.default" class="break-all font-mono">{{
                      entry.default
                    }}</span>
                    <span v-else class="italic">not set</span>
                  </p>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </template>
  </div>
</template>
