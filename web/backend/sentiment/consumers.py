from channels.generic.websocket import AsyncWebsocketConsumer
import json

class SentimentConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.user_id = self.scope["url_route"]["kwargs"]["user_id"]
        self.group_name = f"sentiment_{self.user_id}"
        # Add client to the group
        await self.channel_layer.group_add(self.group_name, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        # Remove client from group
        await self.channel_layer.group_discard(self.group_name, self.channel_name)

    # Handler for "send_sentiment" event sent by kafka_consumer
    async def send_sentiment(self, event):
        # event["message"] contains keys: keyword1, value1, keyword2, value2, timestamp
        await self.send(text_data=json.dumps(event["message"]))