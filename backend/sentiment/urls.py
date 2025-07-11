from django.urls import path
from . import views

urlpatterns = [
    path("api/sentiment/start/", views.sentiment_start, name="sentiment_start"),
    path("api/sentiment/stop/", views.sentiment_stop, name="sentiment_stop"),
]