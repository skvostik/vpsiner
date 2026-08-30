import { onBeforeUnmount, ref } from 'vue'

// Shared 1s clock so every uptime display ticks in sync off a single timer.
const now = ref(Date.now())
let subscribers = 0
let timer: number | undefined

export function useNow() {
  if (subscribers === 0) {
    timer = window.setInterval(() => {
      now.value = Date.now()
    }, 1_000)
  }
  subscribers += 1

  onBeforeUnmount(() => {
    subscribers -= 1
    if (subscribers === 0 && timer) {
      window.clearInterval(timer)
      timer = undefined
    }
  })

  return now
}
