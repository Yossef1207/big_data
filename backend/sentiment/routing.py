# sentiment/routing.py
from django.urls import path
from .consumers import SentimentConsumer

websocket_urlpatterns = [
    path('ws/sentiment/<str:user_id>/', SentimentConsumer.as_asgi()),
]
