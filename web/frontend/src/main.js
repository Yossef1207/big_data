import { createApp } from "vue";
import App from "./App.vue";
import router from "./router";
import "./style/style.scss";

let userId = localStorage.getItem("sentimentUserId");
if (!userId) {
	userId = crypto.randomUUID();
	localStorage.setItem("sentimentUserId", userId);
}

router.beforeEach((to, from, next) => {
	if (to.path === "/") return next(`/sentiment/${userId}`);
	next();
});

createApp(App).provide("userId", userId).use(router).mount("#app");
