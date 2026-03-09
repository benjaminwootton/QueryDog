import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3001,
    allowedHosts: ['querydog.benjaminwootton.com'],
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:3002',
        changeOrigin: true,
        timeout: 10000,
        proxyTimeout: 10000,
      },
    },
  },
})
