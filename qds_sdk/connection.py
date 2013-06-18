import os
import requests
import cjson
import logging

from exception import *


log = logging.getLogger("qds_connection")

class Connection:

    def __init__ (self, auth, base_url):
        self.auth=auth
        self.base_url=base_url
        self._headers = {'Content-Type': 'application/json'}

    def get_raw(self, path, data=None):
        return self._api_call_raw("GET", path, data);

    def get(self, path, data=None):
        return self._api_call("GET", path, data);

    def put(self, path, data=None):
        return self._api_call("PUT", path, data);

    def post(self, path, data=None):
        return self._api_call("POST", path, data);

    def _api_call_raw(self, req_type, path, data=None):
        url = os.path.join(self.base_url, path)
        kwargs = {'headers': self._headers, 'auth': self.auth}
        if data:
            kwargs['data'] = cjson.encode(data)
        log.info("[%s] %s" % (req_type, url))
        if req_type == 'GET':
            r = requests.get(url, **kwargs)
        elif req_type == 'POST':
            r = requests.post(url, **kwargs)
        elif req_type == 'PUT':
            r = requests.put(url, **kwargs)
        else:
            raise NotImplemented

        self._handle_error(r)
        
        return r

    def _api_call(self, req_type, path, data=None):
        return self._api_call_raw(req_type, path, data).json()
    

    def _handle_error(self, request):
        """Raise exceptions in response to any http errors

        Args:
            err: A Request object

        Raises:
            Redirection: if HTTP error code 301,302 returned.
            BadRequest: if HTTP error code 400 returned.
            UnauthorizedAccess: if HTTP error code 401 returned.
            ForbiddenAccess: if HTTP error code 403 returned.
            ResourceNotFound: if HTTP error code 404 is returned.
            MethodNotAllowed: if HTTP error code 405 is returned.
            ResourceConflict: if HTTP error code 409 is returned.
            ResourceInvalid: if HTTP error code 422 is returned.
            ClientError: if HTTP error code falls in 401 - 499.
            ServerError: if HTTP error code falls in 500 - 599.
            ConnectionError: if unknown HTTP error code returned.
        """
        code = request.status_code

        if 200 <= code < 400:
            return

        if code in (301, 302):
            raise Redirection(request)
        elif code == 400:
            raise BadRequest(request)
        elif code == 401:
            raise UnauthorizedAccess(request)
        elif code == 403:
            raise ForbiddenAccess(request)
        elif code == 404:
            raise ResourceNotFound(request)
        elif code == 405:
            raise MethodNotAllowed(request)
        elif code == 409:
            raise ResourceConflict(request)
        elif code == 422:
            raise ResourceInvalid(request)
        elif 401 <= code < 500:
            raise ClientError(request)
        elif 500 <= code < 600:
            raise ServerError(request)
        else:
            raise ConnectionError(request)
