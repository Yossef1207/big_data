<template>
	<header class="app-header">
		<h1>Reddit Sentiment Demo</h1>
	</header>

	<main>
		<router-view />
		<!-- zamiast mainView.vue używamy routera -->
	</main>

	<footer class="app-footer">
		<p>&copy; 2025 Reddit Sentiment Demo</p>
	</footer>
</template>

<script setup>
	import { inject, onMounted } from "vue";

	const userId = inject("userId");

        const backendHost = window.location.hostname;

        onMounted(() => {
                fetch(`http://${backendHost}:8000/api/sentiment/start/`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				user_id: userId,
				keyword1: "vuejs",
				keyword2: "kafka",
			}),
		}).catch((err) => console.error("Nie udało się wystartować sesji:", err));
	});
</script>
