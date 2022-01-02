from collections import OrderedDict
from django.core.cache import cache
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.http.response import HttpResponseBase, JsonResponse, HttpResponse
from django.contrib.postgres.aggregates.general import ArrayAgg
from django.conf import settings
from django.db import transaction, NotSupportedError
from django.db.models import Q, Model
from django.db.models.query import ValuesIterable, QuerySet
from django.utils import timezone

from .exceptions import ApiException
from .serializers import serialize, serialize_relateds
from .basemodel import BaseModel

import orjson
import importlib
import logging
import base64
import zlib

def handle_request(request):
    logger = logging.getLogger("api")
    try:
        try:
            request_obj = orjson.loads(request.body.decode('utf-8'))
        except ValueError:
            raise ApiException('Malformed JSON', 400)

        try:
            handler_path = request_obj['handler']
            args = request_obj.get('args', dict())
        except KeyError as e:
            raise ApiException('"%s" field missing'%e.args[0], 400)

        if type(handler_path) is not str:
            raise ApiException('handler_path field must be a string', 400)
        if type(args) is not dict:
            raise ApiException('data field must be a dict', 400)

        handler_class, name = get_handler_class(handler_path)
        handler = handler_class(name, request)
        if not handler.check_permissions(args):
            raise ApiException('Insufficient privileges', 403)

        if handler.cached and \
           request.headers.get("X-Accept-Cached", "true") == "true" and \
           not settings.DEBUG:
            data = cache.get(handler_path)

            if data:
                response = HttpResponse(data)

                if handler.zlib_compress:
                    response["Content-Encoding"] = "deflate"

                return response

        with transaction.atomic():
            handler_resp = handler.execute_typed(args)

        if isinstance(handler_resp, HttpResponseBase):
            return handler_resp

        if type(handler_resp) is not dict:
            handler_resp = {
                'data': handler_resp
            }

        # crud has its own serialization
        if not handler.prevent_serialization:
            rel_dict = None
            if handler.relateds:
                rel_dict = dict()

            handler_resp = serialize(handler_resp, request.user, rel_dict, [], handler.sanitize)

            if handler.relateds:
                handler_resp["relateds"] = serialize_relateds(rel_dict)

        data = {
            'status': True,
            'generated_on': timezone.now()
        }

        data = {**handler_resp, **data}
        status = 200

    except ValidationError as e:
        logger.error(str(e), extra={'exception_obj': e})
        errors = list()

        # weird django type
        if not hasattr(e, 'error_dict'):
            errors = e.messages
        else:
            for field, error in e.message_dict.items():
                error = ' '.join(error)
                errors.append(f'Error validating field "{field}" : {error}')

        data = {
            'errors': errors,
            'status': False
        }
        status = 400
    except ObjectDoesNotExist as e:
        logger.error(str(e), extra={'exception_obj': e})
        error_message = str(e)
        data = {
            'errors': [error_message],
            'status': False
        }
        status = 404
    except ApiException as e:
        logger.error(str(e), extra={'exception_obj': e})
        error_message = str(e)
        data = {
            'errors': [error_message],
            'status': False
        }
        status = e.status
        #capture_exception(e)
    except Exception as e:
        error_message = "Unexpected internal server error, please contact support."

        if settings.DEBUG:
            error_message = str(e)

        data = {
            'errors': [error_message],
            'status': False
        }
        status = 500
        logger.error(str(e), extra={'exception_obj': e})
        #capture_exception(e)

    try:
        data = orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS)
    except Exception as e:
        error_message = "Unexpected internal server error while sending response, please contact support."

        if settings.DEBUG:
            error_message = str(e)

        data = {
            'errors': [error_message],
            'status': False
        }
        data = orjson.dumps(data)
        status = 500
        logger.error(str(e), extra={'exception_obj': e})

    response = HttpResponse(status=status, content_type="application/json")
    if handler.zlib_compress:
        data = zlib.compress(data)
        response["Content-Encoding"] = "deflate"

    if handler.cached and status == 200 and not settings.DEBUG:
        cache.set(handler_path, data, timeout=handler.cache_timeout)

    response.write(data)
    return response


def get_handler_class(action):
    try:
        args = action.split('.')
        name = args[0] + '.handlers.' + '.'.join(args[1:])
        module = importlib.import_module(name)
        handler_class = module.Handler
    except Exception as e:
        raise ApiException(f"Error while loading '{action}' handler", 400)
    return handler_class, action

