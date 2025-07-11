# sentiment_backend/asgi.py

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.security.websocket import AllowedHostsOriginValidator
from channels.auth import AuthMiddlewareStack
import sentiment.routing as sentiment_routing

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sentiment_backend.settings")

application = ProtocolTypeRouter({
    "http": get_asgi_application(),

    "websocket": AllowedHostsOriginValidator(
        AuthMiddlewareStack(
            URLRouter(
                sentiment_routing.websocket_urlpatterns
            )
        )
    ),
})
