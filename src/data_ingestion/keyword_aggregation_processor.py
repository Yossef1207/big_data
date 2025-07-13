"""
Keyword Aggregation Processor with Timestamp Range Tracking
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, MapFunction
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.datastream.connectors.kafka import KafkaSink, DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import CoProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration constants
KEYWORD_REQUESTS_TOPIC = "keyword_requests"
LABELED_COMMENTS_TOPIC = "labeled-reddit-comments"
KEYWORD_RESPONSES_TOPIC = "keyword_responses"
KAFKA_ENDPOINT = "kafka:9092"
WINDOW_SIZE_COMMENTS = 3000


# ================================================================
# Data Classes for Keyword Requests and Labeled Comments
# ================================================================

@dataclass
class KeywordRequest:
    """Data class for keyword requests"""
    user_id: str
    keyword1: str
    keyword2: str
    request_time: str

    @classmethod
    def from_json(cls, json_str: str) -> Optional['KeywordRequest']:
        try:
            data = json.loads(json_str)
            return cls(
                user_id=data['user_id'],
                keyword1=data['keyword1'],
                keyword2=data['keyword2'],
                request_time=data['request_time']
            )
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse keyword request: {e}")
            return None


@dataclass
class LabeledComment:
    """Data class for labeled comments"""
    id: str
    created_utc: int
    subreddit: str
    score: int
    cleaned_body: str
    label: str
    original_length: int

    @classmethod
    def from_json(cls, json_str: str) -> Optional['LabeledComment']:
        try:
            data = json.loads(json_str)
            return cls(
                id=data['id'],
                created_utc=data['created_utc'],
                subreddit=data['subreddit'],
                score=data['score'],
                cleaned_body=data['cleaned_body'],
                label=data['label'],
                original_length=data['original_length']
            )
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse labeled comment: {e}")
            return None


# ================================================================
# Keyword Matching and Processing Functions
# ================================================================

def check_match(text: str, keyword: str) -> bool:
    """
    Return True if every word in `keyword` appears as a whole word
    in `text` (case‐insensitive, arbitrary whitespace normalized).
    """
    if not text or not keyword:
        return False

    # normalize whitespace & case
    normalized_text = ' '.join(text.lower().split())
    normalized_keyword = ' '.join(keyword.lower().split())

    # for each word in keyword, check as a whole word in text
    for word in normalized_keyword.split():
        # \b ensures we match only at word boundaries
        pattern = rf'\b{re.escape(word)}\b'
        if not re.search(pattern, normalized_text):
            return False

    return True


class KeywordCommentConnector(CoProcessFunction):
    """Connect keyword requests with comments to find matches"""

    def __init__(self):
        self.active_requests_state = None
        self.keyword_matches_state = None
        self.timestamp_ranges_state = None
        self.comment_counter_state = None
        self.first_comment_timestamp_state = None
        self.last_comment_timestamp_state = None

    def open(self, runtime_context):
        # State to store active requests
        requests_desc = MapStateDescriptor(
            "active_requests",
            Types.STRING(),
            Types.STRING()
        )
        self.active_requests_state = runtime_context.get_map_state(requests_desc)

        # State to accumulate matches for each user-keyword pair
        matches_desc = MapStateDescriptor(
            "keyword_matches",
            Types.STRING(),
            Types.STRING()
        )
        self.keyword_matches_state = runtime_context.get_map_state(matches_desc)

        # State to track timestamp ranges for each user-keyword pair
        timestamp_ranges_desc = MapStateDescriptor(
            "timestamp_ranges",
            Types.STRING(),
            Types.STRING()
        )
        self.timestamp_ranges_state = runtime_context.get_map_state(timestamp_ranges_desc)

        # State to track comment count
        comment_counter_desc = ValueStateDescriptor(
            "comment_counter",
            Types.LONG()
        )
        self.comment_counter_state = runtime_context.get_state(comment_counter_desc)

        # State to track first comment timestamp
        first_timestamp_desc = ValueStateDescriptor(
            "first_comment_timestamp",
            Types.LONG()
        )
        self.first_comment_timestamp_state = runtime_context.get_state(first_timestamp_desc)

        # State to track last comment timestamp
        last_timestamp_desc = ValueStateDescriptor(
            "last_comment_timestamp",
            Types.LONG()
        )
        self.last_comment_timestamp_state = runtime_context.get_state(last_timestamp_desc)

        logger.info("KeywordCommentConnector: Initialized state descriptors with timestamp tracking")

    def process_element1(self, value, ctx):
        """Process keyword requests (first stream)"""
        try:
            data = json.loads(value)
            if data.get('type') == 'request':
                user_id = data['user_id']
                request_data = {
                    'keyword1': data['keyword1'],
                    'keyword2': data['keyword2'],
                    'request_time': data['request_time']
                }

                # Store request in state
                self.active_requests_state.put(user_id, json.dumps(request_data))
                logger.info(f"Stored keyword request for user {user_id}: {data['keyword1']}, {data['keyword2']}")

                # Initialize match counters and timestamp ranges for this user's keywords
                self._initialize_match_counters(user_id, request_data['keyword1'], request_data['keyword2'])
                self._initialize_timestamp_ranges(user_id, request_data['keyword1'], request_data['keyword2'])

                # Initialize comment counter if not set
                if self.comment_counter_state.value() is None:
                    self.comment_counter_state.update(0)

        except Exception as e:
            logger.error(f"Error processing keyword request: {e}")

    def process_element2(self, value, ctx):
        """Process labeled comments (second stream)"""
        try:
            data = json.loads(value)
            if data.get('type') == 'comment':
                comment_body = data['cleaned_body']
                comment_subreddit = data['subreddit']
                comment_label = data['label']
                comment_timestamp = data.get('created_utc', 0)

                # Update comment counter and timestamp tracking
                current_count = self.comment_counter_state.value() or 0
                current_count += 1
                self.comment_counter_state.update(current_count)

                # Track first and last comment timestamps in this window
                if self.first_comment_timestamp_state.value() is None:
                    self.first_comment_timestamp_state.update(comment_timestamp)
                self.last_comment_timestamp_state.update(comment_timestamp)

                # Check against all active requests
                if self.active_requests_state:
                    for user_id, request_data_str in self.active_requests_state.items():
                        if request_data_str:
                            request_data = json.loads(request_data_str)
                            keyword1 = request_data.get('keyword1', '')
                            keyword2 = request_data.get('keyword2', '')

                            # Check keyword1 match
                            if keyword1 and (check_match(comment_body, keyword1) or
                                             check_match(comment_subreddit, keyword1)):
                                self._record_match(user_id, keyword1, comment_label)
                                self._update_timestamp_range(user_id, keyword1, comment_timestamp)
                                logger.debug(f"Match found for {user_id}#{keyword1} at timestamp {comment_timestamp}")

                            # Check keyword2 match
                            if keyword2 and (check_match(comment_body, keyword2) or
                                             check_match(comment_subreddit, keyword2)):
                                self._record_match(user_id, keyword2, comment_label)
                                self._update_timestamp_range(user_id, keyword2, comment_timestamp)
                                logger.debug(f"Match found for {user_id}#{keyword2} at timestamp {comment_timestamp}")

                # Check if window is complete
                if current_count >= WINDOW_SIZE_COMMENTS:
                    yield from self._process_window_completion()

        except Exception as e:
            logger.error(f"Error processing comment: {e}")

    def _process_window_completion(self):
        """Called when comment window is complete - send results for all active requests"""
        try:
            first_timestamp = self.first_comment_timestamp_state.value()
            last_timestamp = self.last_comment_timestamp_state.value()
            comment_count = self.comment_counter_state.value()

            logger.info(
                f"Window completed with {comment_count} comments, timestamp range: {first_timestamp} to {last_timestamp}")

            # Send results for all active users
            if self.active_requests_state:
                for user_id, request_data_str in self.active_requests_state.items():
                    if request_data_str:
                        request_data = json.loads(request_data_str)
                        result = self._create_result(user_id, request_data)
                        yield json.dumps(result)
                        logger.info(f"Sent result for user {user_id}: {result}")

            # Clear match data and timestamp ranges for the next window
            self._clear_and_reinitialize_matches()

            # Reset window tracking
            self.comment_counter_state.update(0)
            self.first_comment_timestamp_state.clear()
            self.last_comment_timestamp_state.clear()

            logger.info(f"Reset window counters for next {WINDOW_SIZE_COMMENTS} comments")

        except Exception as e:
            logger.error(f"Error in window completion processing: {e}")

    def _initialize_match_counters(self, user_id: str, keyword1: str, keyword2: str):
        """Initialize match counters for user's keywords"""
        for keyword in [keyword1, keyword2]:
            if keyword:
                key = f"{user_id}#{keyword}"
                initial_data = {
                    'positive': 0,
                    'negative': 0,
                    'neutral': 0,
                    'total': 0
                }
                self.keyword_matches_state.put(key, json.dumps(initial_data))

    def _initialize_timestamp_ranges(self, user_id: str, keyword1: str, keyword2: str):
        """Initialize timestamp ranges for user's keywords"""
        for keyword in [keyword1, keyword2]:
            if keyword:
                key = f"{user_id}#{keyword}"
                initial_range = {
                    'min_timestamp': None,
                    'max_timestamp': None,
                }
                self.timestamp_ranges_state.put(key, json.dumps(initial_range))

    def _record_match(self, user_id: str, keyword: str, label: str):
        """Record a keyword match"""
        key = f"{user_id}#{keyword}"
        match_data = json.loads(self.keyword_matches_state.get(key) or
                                '{"positive": 0, "negative": 0, "neutral": 0, "total": 0}')

        # Update match data
        match_data[label] = match_data.get(label, 0) + 1
        match_data['total'] += 1

        # Store updated data
        self.keyword_matches_state.put(key, json.dumps(match_data))

    def _update_timestamp_range(self, user_id: str, keyword: str, comment_timestamp: int):
        """Update timestamp range for a keyword match"""
        key = f"{user_id}#{keyword}"
        timestamp_range = json.loads(self.timestamp_ranges_state.get(key) or
                                     '{"min_timestamp": null, "max_timestamp": null}')

        timestamp_range['min_timestamp'] = min(timestamp_range['min_timestamp'] or comment_timestamp,
                                               comment_timestamp)
        timestamp_range['max_timestamp'] = max(timestamp_range['max_timestamp'] or comment_timestamp,
                                               comment_timestamp)

        self.timestamp_ranges_state.put(key, json.dumps(timestamp_range))

    def _create_result(self, user_id: str, request_data: dict) -> dict:
        """Create a result for a user including timestamp ranges"""
        keyword1 = request_data.get('keyword1', '')
        keyword2 = request_data.get('keyword2', '')

        value1 = self._calculate_positive_ratio(user_id, keyword1)
        value2 = self._calculate_positive_ratio(user_id, keyword2)

        # Use the last comment timestamp from this window
        last_timestamp = self.last_comment_timestamp_state.value()
        if last_timestamp is not None:
            max_timestamp_iso = datetime.fromtimestamp(last_timestamp, tz=timezone.utc).isoformat().replace('+00:00',
                                                                                                            'Z')
        else:
            max_timestamp_iso = -1

        key1 = f"{user_id}#{keyword1}" if keyword1 else ""
        key2 = f"{user_id}#{keyword2}" if keyword2 else ""

        total1 = json.loads(self.keyword_matches_state.get(key1) or '{}').get('total', 0) if key1 else 0
        total2 = json.loads(self.keyword_matches_state.get(key2) or '{}').get('total', 0) if key2 else 0

        result = {
            'user_id': user_id,
            'keyword1': keyword1,
            'value1': value1,
            'total1': total1,
            'keyword2': keyword2,
            'value2': value2,
            'timestamp': max_timestamp_iso,
            'total2': total2
        }

        return result

    def _get_timestamp_range(self, user_id: str, keyword: str) -> dict:
        """Get timestamp range for a keyword"""
        if not keyword:
            return {'min_timestamp': None, 'max_timestamp': None}

        key = f"{user_id}#{keyword}"
        range_str = self.timestamp_ranges_state.get(key) or '{}'

        try:
            timestamp_range = json.loads(range_str)
            return {
                'min_timestamp': timestamp_range.get('min_timestamp'),
                'max_timestamp': timestamp_range.get('max_timestamp'),
            }
        except Exception as e:
            logger.error(f"Error getting timestamp range: {e}")
            return {'min_timestamp': None, 'max_timestamp': None}

    def _clear_and_reinitialize_matches(self):
        """Clear match data and timestamp ranges, then reinitialize counters for all active requests"""
        try:
            # Get all active requests to reinitialize their counters
            if self.active_requests_state:
                for user_id, request_data_str in self.active_requests_state.items():
                    if request_data_str:
                        request_data = json.loads(request_data_str)
                        keyword1 = request_data.get('keyword1', '')
                        keyword2 = request_data.get('keyword2', '')

                        # Reinitialize counters and timestamp ranges for this user's keywords
                        for keyword in [keyword1, keyword2]:
                            if keyword:
                                key = f"{user_id}#{keyword}"

                                # Reset match counters
                                initial_data = {
                                    'positive': 0,
                                    'negative': 0,
                                    'neutral': 0,
                                    'total': 0
                                }
                                self.keyword_matches_state.put(key, json.dumps(initial_data))

                                # Reset timestamp ranges
                                initial_range = {
                                    'min_timestamp': None,
                                    'max_timestamp': None,
                                }
                                self.timestamp_ranges_state.put(key, json.dumps(initial_range))

            logger.info("Reinitialized match counters and timestamp ranges for all active requests")
        except Exception as e:
            logger.error(f"Error clearing and reinitializing matches: {e}")

    def _calculate_positive_ratio(self, user_id: str, keyword: str) -> float:
        """Calculate positive sentiment ratios for a keyword"""
        try:
            positivity_score = -1

            key = f"{user_id}#{keyword}"
            match_data = json.loads(self.keyword_matches_state.get(key) or '{}')
            total = match_data.get('total', 0)
            positive = match_data.get('positive', 0)
            negative = match_data.get('negative', 0)
            neutral = match_data.get('neutral', 0)

            # Prevent division by zero
            if total != 0:
                # Scale: negative = 0, neutral = 0.5, positive = 1
                weighted_sum = positive * 1 + neutral * 0.5 + negative * 0
                positivity_score = weighted_sum / total

            logger.info(f"Matched {total} comments for {keyword}")

            return positivity_score
        except Exception as e:
            logger.error(f"Error calculating positive ratio: {e}")
            return 0.5


# ================================================================
# Input Processing Functions
# ================================================================

class KeywordRequestProcessor(MapFunction):
    """Process keyword requests"""

    def map(self, value: str) -> str:
        try:
            # Try to parse as KeywordRequest
            request = KeywordRequest.from_json(value)
            if request:
                return json.dumps({
                    'type': 'request',
                    'user_id': request.user_id,
                    'keyword1': request.keyword1,
                    'keyword2': request.keyword2,
                    'request_time': request.request_time
                })

            # If parsing fails, mark as invalid
            return json.dumps({'type': 'invalid_request'})

        except Exception as e:
            logger.error(f"Error processing request: {e}")
            return json.dumps({'type': 'invalid_request'})


class CommentProcessor(MapFunction):
    """Process labeled comments"""

    def map(self, value: str) -> str:
        try:
            # Try to parse as LabeledComment
            comment = LabeledComment.from_json(value)
            if comment:
                return json.dumps({
                    'type': 'comment',
                    'id': comment.id,
                    'created_utc': comment.created_utc,
                    'cleaned_body': comment.cleaned_body,
                    'subreddit': comment.subreddit,
                    'label': comment.label,
                    'score': comment.score
                })

            # If parsing fails, mark as invalid
            return json.dumps({'type': 'invalid_comment'})

        except Exception as e:
            logger.error(f"Error processing comment: {e}")
            return json.dumps({'type': 'invalid_comment'})


# ================================================================
# Main Processor Class
# ================================================================
class KeywordAggregationProcessor:
    """Main processor for keyword-based sentiment aggregation with comment-based windowing"""

    def __init__(self,
                 bootstrap_server: str = KAFKA_ENDPOINT,
                 requests_topic: str = KEYWORD_REQUESTS_TOPIC,
                 comments_topic: str = LABELED_COMMENTS_TOPIC,
                 responses_topic: str = KEYWORD_RESPONSES_TOPIC):
        self.bootstrap_server = bootstrap_server
        self.requests_topic = requests_topic
        self.comments_topic = comments_topic
        self.responses_topic = responses_topic

        # Initialize Flink environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(1)
        self.env.enable_checkpointing(30000)  # 30 seconds

        logger.info("Initialized Keyword Aggregation Processor with Timestamp Tracking")

    def _create_kafka_source(self, topic: str) -> KafkaSource:
        """Create a Kafka source for a given topic"""
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.bootstrap_server) \
            .set_topics(topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest())\
            .build()

    def _create_kafka_sink(self, topic: str) -> KafkaSink:
        """Create Kafka sink for given topic"""
        record_serializer = KafkaRecordSerializationSchema.builder() \
            .set_topic(topic) \
            .set_value_serialization_schema(SimpleStringSchema()) \
            .build()

        return KafkaSink.builder() \
            .set_bootstrap_servers(self.bootstrap_server) \
            .set_record_serializer(record_serializer) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .build()

    def run(self, job_name: Optional[str] = None):
        """Execute the keyword aggregation job"""
        try:
            self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

            # Create Kafka sources
            requests_source = self._create_kafka_source(self.requests_topic)
            comments_source = self._create_kafka_source(self.comments_topic)

            # Create response sink
            responses_sink = self._create_kafka_sink(self.responses_topic)

            # Stream 1: Keyword requests
            requests_stream = self.env.from_source(
                requests_source,
                WatermarkStrategy.no_watermarks(),
                "Keyword_Requests_Source"
            ).map(KeywordRequestProcessor(), output_type=Types.STRING())

            # Stream 2: Labeled comments
            comments_stream = self.env.from_source(
                comments_source,
                WatermarkStrategy.no_watermarks(),
                "Labeled_Comments_Source"
            ).map(CommentProcessor(), output_type=Types.STRING())

            # Connect streams using CoProcessFunction
            connected_stream = requests_stream \
                .key_by(lambda x: "global") \
                .connect(comments_stream.key_by(lambda x: "global")) \
                .process(KeywordCommentConnector(), output_type=Types.STRING())

            # Debug: Print results before sinking
            connected_stream.print("Result")

            # Send to a response topic
            connected_stream.sink_to(responses_sink)

            # Execute job
            job_name = job_name or "KeywordAggregationProcessor"
            logger.info(f"Starting Flink job: {job_name}")
            self.env.execute(job_name)

        except Exception as e:
            logger.error(f"Failed to execute keyword aggregation job: {e}")
            raise


def main():
    """Main entry point"""
    try:
        logger.info("Starting Keyword Aggregation Processor with Comment-Based Windows")
        logger.info(f"Kafka endpoint: {KAFKA_ENDPOINT}")
        logger.info(f"Window size: {WINDOW_SIZE_COMMENTS} comments")

        processor = KeywordAggregationProcessor()
        processor.run()

    except KeyboardInterrupt:
        logger.info("Processor stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


if __name__ == '__main__':
    main()
