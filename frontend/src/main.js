import { createApp } from "vue";
import App from "./App.vue";
import router from "./router";
import "./style/style.scss";
import { v4 as uuidv4 } from 'uuid';

let userId = localStorage.getItem("sentimentUserId");
if (!userId) {
	userId = uuidv4();
	localStorage.setItem("sentimentUserId", userId);
}

router.beforeEach((to, from, next) => {
	if (to.path === "/") return next(`/sentiment/${userId}`);
	next();
});

createApp(App).provide("userId", userId).use(router).mount("#app");
