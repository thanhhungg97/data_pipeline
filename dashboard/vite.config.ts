import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: './',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    chunkSizeWarningLimit: 600,
    // Use esbuild (faster, smaller than terser)
    minify: 'esbuild',
    rollupOptions: {
      output: {
        // Combine everything into fewer files for smaller total size
        manualChunks: (id) => {
          // Bundle all node_modules into vendor chunk
          if (id.includes('node_modules')) {
            return 'vendor';
          }
        },
      },
    },
  },
})
