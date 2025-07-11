<template>
	<section class="bigdata-section">
		<h2>Reddit Sentiment Analysis</h2>

		<div class="controls">
			<input v-model="keyword1" placeholder="Keyword 1" />
			<input v-model="keyword2" placeholder="Keyword 2" />
			<input type="button" value="start" @click="restartSession" />
		</div>

		<div class="chart-wrapper">
			<Line ref="chartRef" :data="chartData" :options="chartOptions" />
		</div>
	</section>
</template>

<script setup>
	import { ref, watch, onMounted, onUnmounted, inject } from "vue";
	import { Line } from "vue-chartjs";
	import {
		Chart,
		LineController,
		LineElement,
		PointElement,
		LinearScale,
		CategoryScale,
		Title,
		Tooltip,
		Legend,
	} from "chart.js";
	Chart.register(LineController, LineElement, PointElement, LinearScale, CategoryScale, Title, Tooltip, Legend);

	const userId = inject("userId");

	const keyword1 = ref("");
	const keyword2 = ref("");

	const chartRef = ref(null);
	const chartData = ref({
		labels: [],
		datasets: [
			{
				label: "Keyword 1",
				data: [],
				borderWidth: 2,
				tension: 0.3,
				pointRadius: 4,
				borderColor: "#a16b01",
				backgroundColor: "rgba(161,107,1,0.2)",
				pointBackgroundColor: "#a16b01",
			},
			{
				label: "Keyword 2",
				data: [],
				borderWidth: 2,
				tension: 0.3,
				pointRadius: 4,
				borderColor: "#f2f2f2",
				backgroundColor: "rgba(242,242,242,0.2)",
				pointBackgroundColor: "#f2f2f2",
			},
		],
	});

	const chartOptions = {
		responsive: true,
		maintainAspectRatio: false,
		scales: {
			y: {
				ticks: { callback: (v) => `${v}%` },
				title: { display: true, text: "% positive" },
				//beginAtZero: true,
			},
		},
		plugins: { legend: { position: "bottom" }, title: { display: false } },
	};

	let socket = null;
	function addPointFromServer({ value1, value2, timestamp }) {
		const timeLabel = new Date(timestamp).toLocaleTimeString("en-EN", { hour12: false });
		const oldLabels = chartData.value.labels;
		const oldData1 = chartData.value.datasets[0].data;
		const oldData2 = chartData.value.datasets[1].data;
		chartData.value = {
			labels: [...oldLabels, timeLabel].slice(-100),
			datasets: [
				{ ...chartData.value.datasets[0], data: [...oldData1, value1 * 100].slice(-100) },
				{ ...chartData.value.datasets[1], data: [...oldData2, value2 * 100].slice(-100) },
			],
		};
		chartRef.value?.chart?.update();
	}

	onMounted(() => {
		socket = new WebSocket(`ws://localhost:8000/ws/sentiment/${userId}/`);
		socket.onmessage = (e) => {
			try {
				addPointFromServer(JSON.parse(e.data));
			} catch {}
		};
	});

	async function restartSession() {
		chartData.value.labels = [];
		chartData.value.datasets[0].data = [];
		chartData.value.datasets[1].data = [];
		await fetch("http://localhost:8000/api/sentiment/stop/", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ user_id: userId }),
		}).catch(() => {});
		await fetch("http://localhost:8000/api/sentiment/start/", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				user_id: userId,
				keyword1: keyword1.value,
				keyword2: keyword2.value,
			}),
		}).catch(() => {});
	}

	// watch([keyword1, keyword2], ([new1, new2], [old1, old2]) => {
	// 	if (new1 !== old1 || new2 !== old2) {
	// 		restartSession();
	// 	}
	// });

	onUnmounted(() => {
		socket?.close();
	});
</script>

<style scoped lang="scss">
	.chart-wrapper {
		position: relative;
		height: 400px;
		width: 100%;
		margin-top: 2rem;
	}
	.bigdata-section {
		display: flex;
		flex-direction: column;
		align-items: center;
		.controls {
			display: flex;
			gap: 1rem;
			margin-bottom: 1.5rem;
		}
		input {
			padding: 0.6rem 0.8rem;
			border: 1px solid #969696;
			border-radius: 0.5rem;
			font-size: 1rem;
			min-width: 140px;
		}
	}
</style>
