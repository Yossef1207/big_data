## Reddit Sentiment Analysis

This app allows you to analyse sentiment trends for two keywords on Reddit using historical data from 2019.

### How it works

1. **Enter Keywords**
   Provide two keywords you want to compare (e.g. "you", "I", "war", "trump").
2. **Start Session**
   Click **Start** to begin. The frontend sends your keywords to the backend via REST.
3. **Data Collection**
   The backend polls a Kafka topic, retrieving batches of **1,000 Reddit comments** every minute for each keyword.
4. **WebSocket Updates**
   Sentiment scores (`value1`, `value2`) and comment counts (`total1`, `total2`) are pushed to the frontend over WebSockets.
5. **Cumulative Average**
   To reduce noise, each data point on the chart is a running average over all previous batches. After \~10–15 points, the curve stabilises.
6. **Chart Display**
   The chart plots "% positive" over time. Each label shows the full timestamp (YYYY‑MM‑DD HH\:mm\:ss).

### Notes & Limitations

* **Batch Size and Coverage**
  Each data point is based on 1,000 Reddit comments. Niche or low-frequency keywords may not appear in every batch and can result in missing or zero values for those intervals.
* **Historical Scope (2019 Only)**
  The app analyses comments exclusively from 2019. Keywords that trended before or after that year (e.g., "Harris", "Tate", "Ukraine War", "Israel", "Hamas") are likely underrepresented or absent in the dataset.
* **Statistical Fluctuations**
  Raw sentiment from small samples can be noisy—one highly positive or negative comment can skew a single interval. To address this, the app computes a **cumulative average**, where each new point factors in all previous batches. After 10–15 intervals, the sentiment curve stabilises, reflecting the true underlying trend.
* **Zero or Null Intervals**
  If a keyword is missing entirely in a batch, the front end will plot a gap (`null`) for that timestamp. The cumulative average will still carry over from prior intervals, ensuring continuity.
* **Latency and Real-Time Approximation**
  Although data is pushed every minute, network and processing delays can cause slight latency. The timestamp labels reflect when the comment batch was generated, not when it was displayed.