import logging
import traceback


class ApiException(Exception):
    def __init__(self, message, status, log_write=True, critical=False, extra_info=None):
        super().__init__(message)
        self.status = status
        if log_write:
            logger = logging.getLogger("api")
            extra = {
                'exception_obj': self,
                'extra_info': extra_info
            }
            if critical:
                logger.critical(message, extra=extra)
            else:
                logger.error(message, extra=extra)


class ForbiddenApiException(ApiException):
    def __init__(self, message, user):
        username = '"anonym"' if user.is_anonymous else '"%s"'%user.username
        super().__init__('Forbidden for ' + username + ': Insufficient privileges', 403, log_write=False)


class ObjectNotFoundApiException(ApiException):
    def __init__(self, message):
        super().__init__(message, 404)


class AssemblyException(ApiException):
    def __init__(self, message):
        if message:
            super().__init__(f"Assembly Error: {message}", 400, False)
        else:
            super().__init__(f"Assembly Error", 400, False)
