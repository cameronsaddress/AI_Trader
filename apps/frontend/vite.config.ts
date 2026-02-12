import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vitejs.dev/config/
export default defineConfig({
    plugins: [react()],
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./src"),
        },
    },
    server: {
        port: 3112,
        host: true,
        proxy: {
            '/socket.io': {
                target: 'http://backend:5114',
                ws: true,
                changeOrigin: false,
                rewrite: (path) => path
            },
            '/api': {
                target: 'http://backend:5114',
                changeOrigin: true
            }
        }
    }
})
