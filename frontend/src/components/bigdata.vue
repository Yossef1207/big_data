<template>
	<section class="bigdata-section">
		<h2>Reddit Sentiment Analysis</h2>

		<div class="controls">
			<input v-model="keyword1" placeholder="Keyword 1" />
			<input v-model="keyword2" placeholder="Keyword 2" />
			<button @click="restartSession">Start</button>
		</div>

		<div class="chart-wrapper">
			<Line ref="chartRef" :data="chartData" :options="chartOptions" />
		</div>
	</section>
</template>

<script setup>
	import { ref, onMounted, onUnmounted, inject } from "vue";
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
				spanGaps: false,
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
				spanGaps: false,
			},
		],
	});

	const chartOptions = {
		responsive: true,
		maintainAspectRatio: false,
		scales: {
			y: {
				ticks: {
					callback: (v) => `${v}%`,
				},
				title: {
					display: true,
					text: "% positive",
				},
				suggestedMin: 0,
				suggestedMax: 100,
			},
		},
		plugins: {
			legend: { position: "bottom" },
			title: { display: false },
		},
	};

	function toNumber(val) {
		return typeof val === "string" ? Number(val) : val;
	}

	function sanitize(val) {
		const num = toNumber(val);
		return num === -1 ? null : num;
	}

	let cumulativeTotal1 = 0;
	let cumulativeTotal2 = 0;
	let cumulativePositive1 = 0;
	let cumulativePositive2 = 0;

	function addPointFromServer({ value1, value2, total1, total2, timestamp }) {
		const v1 = sanitize(value1);
		const v2 = sanitize(value2);
		const t1 = toNumber(total1);
		const t2 = toNumber(total2);
		console.debug({
			v1,
			total1,
			t1,
			cumulativeTotal1,
			cumulativePositive1,
			v2,
			total2,
			t2,
			cumulativeTotal2,
			cumulativePositive2,
		});

		if (v1 !== null && !Number.isNaN(t1) && t1 > 0) {
			cumulativePositive1 += v1 * t1;
			cumulativeTotal1 += t1;
		}
		if (v2 !== null && !Number.isNaN(t2) && t2 > 0) {
			cumulativePositive2 += v2 * t2;
			cumulativeTotal2 += t2;
		}

		const avg1 = cumulativeTotal1 ? (cumulativePositive1 / cumulativeTotal1) * 100 : null;
		const avg2 = cumulativeTotal2 ? (cumulativePositive2 / cumulativeTotal2) * 100 : null;

		const timeLabel = new Date(timestamp).toLocaleString("en-EN", {
			year: "numeric",
			month: "2-digit",
			day: "2-digit",
			hour: "2-digit",
			minute: "2-digit",
			second: "2-digit",
			hour12: false,
		});

		chartData.value = {
			labels: [...chartData.value.labels, timeLabel].slice(-100),
			datasets: [
				{
					...chartData.value.datasets[0],
					data: [...chartData.value.datasets[0].data, avg1].slice(-100),
				},
				{
					...chartData.value.datasets[1],
					data: [...chartData.value.datasets[1].data, avg2].slice(-100),
				},
			],
		};

		chartRef.value?.chart?.update();

		console.debug("Point added", { timeLabel, v1, t1, cumulativeTotal1, avg1, v2, t2, cumulativeTotal2, avg2 });
	}

	let socket = null;
	const backendHost = window.location.hostname;

	onMounted(() => {
		socket = new WebSocket(`ws://${backendHost}:8000/ws/sentiment/${userId}/`);

		socket.onmessage = (event) => {
			try {
				const payload = JSON.parse(event.data);
				console.debug(payload);
				addPointFromServer(payload);
			} catch (err) {
				console.error("Malformed WS message", err);
			}
		};
	});

	onUnmounted(() => {
		socket?.close();
	});

	async function restartSession() {
		chartData.value.labels = [];
		chartData.value.datasets[0].data = [];
		chartData.value.datasets[1].data = [];
		chartData.value.datasets[0].label = keyword1.value || "Keyword 1";
		chartData.value.datasets[1].label = keyword2.value || "Keyword 2";
		cumulativeTotal1 = 0;
		cumulativeTotal2 = 0;
		cumulativePositive1 = 0;
		cumulativePositive2 = 0;
		chartRef.value?.chart?.update();

		const opts = {
			method: "POST",
			headers: { "Content-Type": "application/json" },
		};

		try {
			await fetch(`http://${backendHost}:8000/api/sentiment/stop/`, {
				...opts,
				body: JSON.stringify({ user_id: userId }),
			});
		} catch {}

		try {
			await fetch(`http://${backendHost}:8000/api/sentiment/start/`, {
				...opts,
				body: JSON.stringify({ user_id: userId, keyword1: keyword1.value, keyword2: keyword2.value }),
			});
		} catch {}
	}
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
