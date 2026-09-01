import { createRouter, createWebHistory } from 'vue-router'

import HostView from './views/HostView.vue'

export const router = createRouter({
  history: createWebHistory(),
  scrollBehavior: (to, from, savedPosition) => savedPosition ?? { top: 0 },
  routes: [
    { path: '/', name: 'host', component: HostView },
    {
      path: '/containers',
      name: 'containers',
      component: () => import('./views/ContainersView.vue'),
    },
    {
      path: '/containers/:id',
      name: 'container-detail',
      component: () => import('./views/ContainerDetailView.vue'),
    },
    { path: '/logs', name: 'logs', component: () => import('./views/ServicesView.vue') },
    {
      path: '/logs/:service',
      name: 'log-viewer',
      component: () => import('./views/LogsView.vue'),
    },
    {
      path: '/configuration',
      name: 'configuration',
      component: () => import('./views/ConfigurationView.vue'),
    },
  ],
})
