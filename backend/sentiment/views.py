import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .sessions import start_session, stop_existing_session
import logging

@csrf_exempt
def sentiment_start(request):
    """
    POST /api/sentiment/start/
    Body: { "user_id": "<id>", "keyword1": "...", "keyword2": "..." }
    """
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    try:
        data = json.loads(request.body or "{}")
        user_id = data["user_id"]
        keyword1 = data["keyword1"]
        keyword2 = data["keyword2"]
    except (KeyError, json.JSONDecodeError):
        return JsonResponse({"error": "Invalid payload"}, status=400)

    start_session(user_id, keyword1, keyword2)
    logger = logging.getLogger(__name__)
    logger.debug("Received /api/sentiment/start for user=%s kw1=%s kw2=%s", user_id, keyword1, keyword2)
    start_session(user_id, keyword1, keyword2)
    return JsonResponse({"status": "started"})

@csrf_exempt
def sentiment_stop(request):
    """
    POST /api/sentiment/stop/
    Body: { "user_id": "<id>" }
    """
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    try:
        data = json.loads(request.body or "{}")
        user_id = data["user_id"]
    except (KeyError, json.JSONDecodeError):
        return JsonResponse({"error": "Invalid payload"}, status=400)

    stop_existing_session(user_id)
    return JsonResponse({"status": "stopped"})