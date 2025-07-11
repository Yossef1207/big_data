<template>
	<section class="bigdata-section">
		<h2>Reddit Sentiment Analysis</h2>

		<div class="controls">
			<input v-model="keyword1" placeholder="Keyword 1" />
			<input v-model="keyword2" placeholder="Keyword 2" />
		</div>

		<div class="chart-wrapper">
			<Line ref="chartRef" :data="chartData" :options="chartOptions" />
		</div>
	</section>
</template>

<script setup>
	import { ref, watch, onMounted, onUnmounted } from "vue";
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
				ticks: {
					callback: (v) => `${v}%`,
				},
				title: {
					display: true,
					text: "% positive",
				},
			},
		},
		plugins: {
			legend: { position: "bottom" },
			title: { display: false },
		},
	};

	function randomPercent() {
		return Math.floor(Math.random() * 100) + 0; // [0, 100]
	}

	function addPoint() {
		const now = new Date().toLocaleTimeString("en-EN", { hour12: false });

		const oldLabels = chartData.value.labels;
		const oldData1 = chartData.value.datasets[0].data;
		const oldData2 = chartData.value.datasets[1].data;

		const newLabels = [...oldLabels, now].slice(-100);
		const newData1 = [...oldData1, randomPercent()].slice(-100);
		const newData2 = [...oldData2, randomPercent()].slice(-100);

		chartData.value = {
			labels: newLabels,
			datasets: [
				{
					label: chartData.value.datasets[0].label,
					data: newData1,
					borderWidth: 2,
					tension: 0.3,
					pointRadius: 4,
					borderColor: "#a16b01",
					backgroundColor: "rgba(161,107,1,0.2)",
					pointBackgroundColor: "#a16b01",
				},
				{
					label: chartData.value.datasets[1].label,
					data: newData2,
					borderWidth: 2,
					tension: 0.3,
					pointRadius: 4,
					borderColor: "#f2f2f2",
					backgroundColor: "rgba(242,242,242,0.2)",
					pointBackgroundColor: "#f2f2f2",
				},
			],
		};
	}

	let intervalId = null;

	onMounted(() => {
		addPoint();
		intervalId = setInterval(addPoint, 500);
	});

	onUnmounted(() => {
		if (intervalId) clearInterval(intervalId);
	});

	watch(keyword1, (newVal) => {
		chartData.value = {
			labels: [...chartData.value.labels],
			datasets: [
				{
					label: newVal || "Keyword 1",
					data: [...chartData.value.datasets[0].data],
					borderWidth: 2,
					tension: 0.3,
					pointRadius: 4,
					borderColor: "#a16b01",
					backgroundColor: "rgba(161,107,1,0.2)",
					pointBackgroundColor: "#a16b01",
				},
				{
					label: chartData.value.datasets[1].label,
					data: [...chartData.value.datasets[1].data],
					borderWidth: 2,
					tension: 0.3,
					pointRadius: 4,
					borderColor: "#f2f2f2",
					backgroundColor: "rgba(242,242,242,0.2)",
					pointBackgroundColor: "#f2f2f2",
				},
			],
		};
	});

	watch(keyword2, (newVal) => {
		chartData.value = {
			labels: [...chartData.value.labels],
			datasets: [
				{
					label: chartData.value.datasets[0].label,
					data: [...chartData.value.datasets[0].data],
					borderWidth: 2,
					tension: 0.3,
					pointRadius: 4,
					borderColor: "#a16b01",
					backgroundColor: "rgba(161,107,1,0.2)",
					pointBackgroundColor: "#a16b01",
				},
				{
					label: newVal || "Keyword 2",
					data: [...chartData.value.datasets[1].data],
					borderWidth: 2,
					tension: 0.3,
					pointRadius: 4,
					borderColor: "#f2f2f2",
					backgroundColor: "rgba(242,242,242,0.2)",
					pointBackgroundColor: "#f2f2f2",
				},
			],
		};
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

		h2 {
			margin-bottom: 1.5rem;
		}

		.controls {
			display: flex;
			flex-wrap: wrap;
			justify-content: center;
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
