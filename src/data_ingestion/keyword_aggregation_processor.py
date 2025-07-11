"""
Keyword Aggregation Processor

Author: Veronika Anokhina
Version: 1
"""

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
import re

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, MapFunction
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.datastream.connectors.kafka import KafkaSink, DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import ProcessWindowFunction, KeyedProcessFunction, CoProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration constants
KEYWORD_REQUESTS_TOPIC = "keyword_requests"
LABELED_COMMENTS_TOPIC = "labeled-reddit-comments"
KEYWORD_RESPONSES_TOPIC = "keyword_responses"
KAFKA_ENDPOINT = "kafka:9092"
WINDOW_SIZE_SECONDS = 10


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


class KeywordRequestToCommentKey(MapFunction):
    """Convert keyword requests to a format that can be connected with comments"""

    def map(self, value: str) -> str:
        request = KeywordRequest.from_json(value)
        if request:
            return json.dumps({
                'type': 'request',
                'user_id': request.user_id,
                'keyword1': request.keyword1,
                'keyword2': request.keyword2,
                'request_time': request.request_time
            })
        return json.dumps({'type': 'invalid_request'})


class CommentToCommentKey(MapFunction):
    """Convert comments to a format that can be connected with requests"""

    def map(self, value: str) -> str:
        comment = LabeledComment.from_json(value)
        if comment:
            return json.dumps({
                'type': 'comment',
                'id': comment.id,
                'created_utc': comment.created_utc,
                'subreddit': comment.subreddit,
                'score': comment.score,
                'cleaned_body': comment.cleaned_body,
                'label': comment.label,
                'original_length': comment.original_length
            })
        return json.dumps({'type': 'invalid_comment'})


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
            logger.debug(f"Word '{word}' not found as whole word in '{normalized_text[:50]}…'")
            return False

    logger.debug(f"All words from '{normalized_keyword}' matched in text.")
    return True


class KeywordCommentConnector(CoProcessFunction):
    """Connect keyword requests with comments to find matches"""

    def __init__(self):
        self.active_requests_state = None

    def open(self, runtime_context):
        shared_desc = MapStateDescriptor(
            "shared_active_requests",
            Types.STRING(),
            Types.STRING()
        )
        self.active_requests_state = runtime_context.get_map_state(shared_desc)

        last_req_desc = MapStateDescriptor(
            "last_request",
            Types.STRING(),
            Types.STRING()
        )
        self.last_request_state = runtime_context.get_map_state(last_req_desc)

        logger.info("KeywordCommentConnector: Initialized active_requests_state & last_request_state")

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

                # Store request in shared state
                self.active_requests_state.put(user_id, json.dumps(request_data))
                logger.info(f"Stored keyword request for user {user_id}: {data['keyword1']}, {data['keyword2']}")
                self.last_request_state.put('request', json.dumps(request_data))


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
                comment_score = data['score']
                comment_id = data['id']

                # Check against all active requests
                matches = []
                if self.active_requests_state:
                    try:
                        # Go through each active request
                        for user_id, request_data_str in self.active_requests_state.items():
                            if request_data_str:
                                request_data = json.loads(request_data_str)
                                keyword1 = request_data.get('keyword1', '')
                                keyword2 = request_data.get('keyword2', '')

                                # Check keyword1 match
                                if keyword1 and (check_match(comment_body, keyword1) or
                                                 check_match(comment_subreddit, keyword1)):
                                    matches.append({
                                        'user_id': user_id,
                                        'keyword': keyword1,
                                        'label': comment_label,
                                        'score': comment_score,
                                        'comment_id': comment_id,
                                        'subreddit': comment_subreddit
                                    })

                                # Check keyword2 match
                                if keyword2 and (check_match(comment_body, keyword2) or
                                                 check_match(comment_subreddit, keyword2)):
                                    matches.append({
                                        'user_id': user_id,
                                        'keyword': keyword2,
                                        'label': comment_label,
                                        'score': comment_score,
                                        'comment_id': comment_id,
                                        'subreddit': comment_subreddit
                                    })

                        logger.debug(f"Found {len(matches)} matches for comment {comment_id}")

                        # Emit matches
                        for match in matches:
                            yield json.dumps(match)

                    except Exception as e:
                        logger.error(f"Error checking matches: {e}")

        except Exception as e:
            logger.error(f"Error processing comment: {e}")


class SentimentAggregationWindow(ProcessWindowFunction):
    """Aggregate sentiment matches within time windows"""

    def process(self, key, context, elements):
        try:
            logger.info(f"Processing window for key: {key}")

            # Parse key to get user_id and keyword
            user_id, keyword = key.split('#', 1)

            # Count sentiments
            sentiment_counts = defaultdict(int)
            total_matches = 0
            comment_ids = set()

            for match in elements:
                try:
                    if isinstance(match, str):
                        match = json.loads(match)
                    sentiment_counts[match['label']] += 1
                    total_matches += 1
                    comment_ids.add(match['comment_id'])
                except Exception as e:
                    logger.warning(f"Failed to parse match: {e}")

            if total_matches > 0:
                positive_count = sentiment_counts.get('positive', 0)
                positive_ratio = positive_count / total_matches

                # Create result with additional metadata for debugging
                result = {
                    'user_id': user_id,
                    'keyword': keyword,
                    'positive_ratio': positive_ratio,
                    'total_matches': total_matches,
                    'sentiment_breakdown': dict(sentiment_counts),
                    'unique_comments': len(comment_ids),
                    'window_end': context.window().end,
                    'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                }

                logger.info(
                    f"Window result for {user_id}#{keyword}: {positive_ratio:.2%} positive ({total_matches} matches)")
                yield json.dumps(result)

        except Exception as e:
            logger.error(f"Error in sentiment aggregation: {e}")


class ResultCombiner(KeyedProcessFunction):
    """Combine results for both keywords of each user and send final response"""

    def __init__(self):
        self.keyword_results_state = None
        self.last_request_state = None

    def open(self, runtime_context):
        kw_res_desc = MapStateDescriptor(
            "keyword_results",
            Types.STRING(),
            Types.STRING()
        )
        self.keyword_results_state = runtime_context.get_map_state(kw_res_desc)

        last_req_desc = MapStateDescriptor(
            "last_request",
            Types.STRING(),
            Types.STRING()
        )
        self.last_request_state = runtime_context.get_map_state(last_req_desc)

        logger.info("ResultCombiner: Initialized keyword_results_state & last_request_state")

    def process_element(self, value, ctx):
        partial = json.loads(value)
        user_id   = partial['user_id']
        keyword   = partial['keyword']
        ratio     = partial['positive_ratio']

        self.keyword_results_state.put(keyword, value)

        req_json  = self.last_request_state.get('request')
        if req_json:
            req      = json.loads(req_json)
            kw1, kw2 = req['keyword1'], req['keyword2']
        else:
            keys     = list(self.keyword_results_state.keys())
            kw1, kw2 = (keys + ["", ""])[:2]

        v1 = v2 = 0.5

        res1 = self.keyword_results_state.get(kw1)
        res2 = self.keyword_results_state.get(kw2)
        if res1: v1 = json.loads(res1)['positive_ratio']
        if res2: v2 = json.loads(res2)['positive_ratio']

        yield json.dumps({
            'user_id': user_id,
            'keyword1': kw1,
            'value1'  : v1,
            'keyword2': kw2,
            'value2'  : v2,
            'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        })


# ================================================================
# Main Processor Class
# ================================================================
class KeywordAggregationProcessor:
    """Main processor for keyword-based sentiment aggregation"""

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

        logger.info("Initialized Keyword Aggregation Processor")

    def _create_kafka_source(self, topic: str) -> KafkaSource:
        """Create Kafka source for given topic"""
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.bootstrap_server) \
            .set_topics(topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
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
            ).map(KeywordRequestToCommentKey())

            # Stream 2: Labeled comments
            comments_stream = self.env.from_source(
                comments_source,
                WatermarkStrategy.no_watermarks(),
                "Labeled_Comments_Source"
            ).map(CommentToCommentKey())

            # Connect streams to find keyword matches
            matches_stream = requests_stream \
                .key_by(lambda x: "global") \
                .connect(comments_stream.key_by(lambda x: "global")) \
                .process(KeywordCommentConnector())

            # Window aggregation by user_id#keyword
            #TODO timer
            windowed_results = matches_stream \
                .map(lambda x: json.loads(x)) \
                .key_by(lambda match: f"{match['user_id']}#{match['keyword']}") \
                .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS))) \
                .process(SentimentAggregationWindow(), output_type=Types.STRING())

            # Combine results for each user
            final_responses = windowed_results \
                .key_by(lambda x: json.loads(x)['user_id']) \
                .process(ResultCombiner(), output_type=Types.STRING())

            # Send to response topic
            final_responses.sink_to(responses_sink)

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
        logger.info("Starting Keyword Aggregation Processor")
        logger.info(f"Kafka endpoint: {KAFKA_ENDPOINT}")
        logger.info(f"Window size: {WINDOW_SIZE_SECONDS} seconds")

        processor = KeywordAggregationProcessor()
        processor.run()

    except KeyboardInterrupt:
        logger.info("Processor stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


if __name__ == '__main__':
    main()