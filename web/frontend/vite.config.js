import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import path from "path";

export default defineConfig({
	plugins: [vue()],
	resolve: {
		alias: { "@": path.resolve(__dirname, "src") },
	},
	server: {
		host: true,
		port: 5173,
		strictPort: true,
		hmr: {
			host: "localhost",
			port: 5173,
		},
		watch: {
			usePolling: true,
			interval: 100,
		},
	},
	css: {
		preprocessorOptions: {
			scss: {
				//additionalData: `@use "@/style/style.scss" as *;`,
			},
		},
	},
});
