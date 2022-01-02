from uuid import uuid4
from django.db.models.base import ModelBase
from django.conf import settings
from .exceptions import ApiException
import logging
import inspect


class BaseHandler():
    relateds = False # Fetch all relateds objects
    prevent_serialization = False # Use only orjson base serialization
    zlib_compress = False # Compress body using zlib
    sanitize = True # Sanitize the output or not
    cached = False # Cache the response in Django cache backend
    cache_timeout = settings.CACHE_DEFAULT_TIMEOUT # secs

    def __init__(self, name, request):
        self.name = name
        self.request = request
        self.user = request.user
        self.request.batch_id = uuid4()

    @property
    def logger(self):
        return logging.getLogger("api")

    def check_permissions(self, args):
        if not self.request.user.is_authenticated:
            raise ApiException("User not authenticated", 401)

        splitted_name = self.name.split(".")
        app = splitted_name[0]
        handler_name = "__".join(splitted_name[1:])

        return f"handler:{app}__{handler_name}" in self.request.session.get("permissions", [])

    def execute(self, **kwargs):
        raise NotImplementedError()

    def execute_typed(self, kwargs):
        signature = inspect.signature(self.execute)
        parameters = signature.parameters

        for key, value in kwargs.items():
            if not key in parameters:
                raise Exception(f"Unexpected argument {key} in handler {self.name}.")

            param = parameters[key]

            if type(param.annotation) is ModelBase and type(value) is str:
                qs = param.annotation.objects.filter(pk=value)
                if hasattr(qs, "select_subclasses"):
                    qs = qs.select_subclasses()
                kwargs[key] = qs.get()

        return self.execute(**kwargs)
