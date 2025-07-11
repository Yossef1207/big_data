import { createRouter, createWebHistory } from "vue-router";
import BigData from "@/components/bigdata.vue";

const routes = [
	{
		path: "/sentiment/:userId",
		name: "bigdata",
		component: BigData,
		props: true,
	},
];

export default createRouter({
	history: createWebHistory(import.meta.env.BASE_URL),
	routes,
	scrollBehavior(to, from, saved) {
		return saved || { top: 0 };
	},
});
