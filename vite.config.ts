import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import { componentTagger } from "lovable-tagger";

// https://vitejs.dev/config/
//
// Production SPA deployment: the host must serve index.html for all non-file
// paths so that client-side routing (/jobs/:id) works on refresh/deep-link.
//   Nginx:      try_files $uri $uri/ /index.html;
//   CloudFront: custom error response 403/404 → /index.html with HTTP 200
//   Netlify:    _redirects: /* /index.html 200
export default defineConfig(({ mode }) => ({
  server: {
    host: "::",
    port: 8080,
    hmr: {
      overlay: false,
    },
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  plugins: [react(), mode === "development" && componentTagger()].filter(Boolean),
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
}));
