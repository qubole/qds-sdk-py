import sys
import requests
import cjson
import logging
import ssl
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
from retry import retry
from exception import *


log = logging.getLogger("qds_connection")

"""
see http://stackoverflow.com/questions/14102416/python-requests-requests-exceptions-sslerror-errno-8-ssl-c504-eof-occurred
"""
class MyAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize,
                         block=False):
        self.poolmanager = PoolManager(num_pools=connections,
                                       maxsize=maxsize,
                                       block=block,
                                       ssl_version=ssl.PROTOCOL_TLSv1)


class Connection:

    def __init__(self, auth, base_url, skip_ssl_cert_check, reuse=True):
        self.auth = auth
        self.base_url = base_url
        self.skip_ssl_cert_check = skip_ssl_cert_check
        self._headers = {'Content-Type': 'application/json'}

        self.reuse = reuse
        if reuse:
            self.session = requests.Session()
            self.session.mount('https://', MyAdapter())

    def get_raw(self, path, data=None):
        return self._api_call_raw("GET", path, data)

    @retry(RetryWithDelay, tries=5, delay=20, backoff=2)
    def get(self, path, data=None):
        return self._api_call("GET", path, data)

    def put(self, path, data=None):
        return self._api_call("PUT", path, data)

    def post(self, path, data=None):
        return self._api_call("POST", path, data)

    def delete(self, path, data=None):
        return self._api_call("DELETE", path, data)

    def _api_call_raw(self, req_type, path, data=None):
        url = self.base_url.rstrip('/') + '/' + path

        if self.reuse:
            x = self.session
        else:
            x = requests

        kwargs = {'headers': self._headers, 'auth': self.auth, 'verify': not self.skip_ssl_cert_check}

        if data:
            kwargs['data'] = cjson.encode(data)

        log.info("[%s] %s" % (req_type, url))
        log.info("Payload: %s" % json.dumps(data, indent=4))

        if req_type == 'GET':
            r = x.get(url, **kwargs)
        elif req_type == 'POST':
            r = x.post(url, **kwargs)
        elif req_type == 'PUT':
            r = x.put(url, **kwargs)
        elif req_type == 'DELETE':
            r = x.delete(url, **kwargs)
        else:
            raise NotImplemented

        self._handle_error(r)
        return r

    def _api_call(self, req_type, path, data=None):
        return self._api_call_raw(req_type, path, data).json()

    def _handle_error(self, response):
        """Raise exceptions in response to any http errors

        Args:
            response: A Response object

        Raises:
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
        code = response.status_code

        if 200 <= code < 400:
            return

        if code == 400:
            sys.stderr.write(response.text + "\n")
            raise BadRequest(response)
        elif code == 401:
            sys.stderr.write(response.text + "\n")
            raise UnauthorizedAccess(response)
        elif code == 403:
            sys.stderr.write(response.text + "\n")
            raise ForbiddenAccess(response)
        elif code == 404:
            sys.stderr.write(response.text + "\n")
            raise ResourceNotFound(response)
        elif code == 405:
            sys.stderr.write(response.text + "\n")
            raise MethodNotAllowed(response)
        elif code == 409:
            sys.stderr.write(response.text + "\n")
            raise ResourceConflict(response)
        elif code == 422:
            sys.stderr.write(response.text + "\n")
            raise ResourceInvalid(response)
        elif code in (449, 503):
            sys.stderr.write(response.text + "\n")
            raise RetryWithDelay(response)
        elif 401 <= code < 500:
            sys.stderr.write(response.text + "\n")
            raise ClientError(response)
        elif 500 <= code < 600:
            sys.stderr.write(response.text + "\n")
            raise ServerError(response)
        else:
            raise ConnectionError(response)
