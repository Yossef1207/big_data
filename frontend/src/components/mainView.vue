<template>
	<div id="container-main">
		<div id="main-1">
			<router-view />
		</div>
	</div>
</template>

<script setup></script>

<style scoped lang="scss">
	#container-main {
		display: flex;
		justify-content: center;

		#main-1 {
			width: 100vw;
			max-width: 1400px;
			padding: 2rem 1rem;
		}
	}
</style>
