import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,                           // listen on all interfaces (good for tunnels)
    port: 5173,
    strictPort: true,
    // allow any *.ngrok-free.app (and Cloudflare tunnels) + localhost
    allowedHosts: ['.ngrok-free.app', '.trycloudflare.com', 'localhost']
    // If you still get HMR issues via tunnels, uncomment and set host dynamically:
    // hmr: { protocol: 'wss', clientPort: 443 }
  },
})
