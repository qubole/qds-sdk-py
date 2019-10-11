import sys
import requests
import logging
import ssl
import json
import pkg_resources
from requests.adapters import HTTPAdapter
from datetime import datetime
try:
    from requests.packages.urllib3.poolmanager import PoolManager
except ImportError:
    from urllib3.poolmanager import PoolManager
from qds_sdk.retry import retry
from qds_sdk.exception import *


log = logging.getLogger("qds_connection")

"""
see http://stackoverflow.com/questions/14102416/python-requests-requests-exceptions-sslerror-errno-8-ssl-c504-eof-occurred
"""


class MyAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        super(MyAdapter, self).__init__(*args, **kwargs)

    def init_poolmanager(self, connections, maxsize,block=False):
        self.poolmanager = PoolManager(num_pools=connections,
                                       maxsize=maxsize,
                                       block=block,
                                       ssl_version=ssl.PROTOCOL_SSLv23)


class Connection:

    def __init__(self, auth, rest_url, skip_ssl_cert_check, reuse=True):
        self.auth = auth
        self.rest_url = rest_url
        self.skip_ssl_cert_check = skip_ssl_cert_check
        self._headers = {'User-Agent': 'qds-sdk-py-%s' % pkg_resources.get_distribution("qds-sdk").version,
                         'Content-Type': 'application/json'}

        self.reuse = reuse
        if reuse:
            self.session = requests.Session()
            self.session.mount('https://', MyAdapter())

            # retries for get requests
            self.session_with_retries = requests.Session()
            self.session_with_retries.mount('https://', MyAdapter(max_retries=3))

    @retry((RetryWithDelay, requests.Timeout), tries=6, delay=30, backoff=2)
    def get_raw(self, path, params=None):
        return self._api_call_raw("GET", path, params=params)

    @retry((RetryWithDelay, requests.Timeout), tries=6, delay=30, backoff=2)
    def get(self, path, params=None):
        return self._api_call("GET", path, params=params)

    def put(self, path, data=None):
        return self._api_call("PUT", path, data)

    def post(self, path, data=None):
        return self._api_call("POST", path, data)

    def delete(self, path, data=None):
        return self._api_call("DELETE", path, data)

    def _api_call_raw(self, req_type, path, data=None, params=None):
        url = self.rest_url.rstrip('/') + '/' + path

        if self.reuse:
            x = self.session
            x_with_retries = self.session_with_retries
        else:
            x = requests
            x_with_retries = requests.Session()
            x_with_retries.mount('https://', MyAdapter(max_retries=3))

        kwargs = {'headers': self._headers, 'auth': self.auth, 'verify': not self.skip_ssl_cert_check}

        if data:
            kwargs['data'] = json.dumps(data)
        if params:
            kwargs['params'] = params

        log.info("[%s] %s" % (req_type, url))
        log.info("Payload: %s" % json.dumps(data, indent=4))
        log.info("Params: %s" % params)

        if req_type == 'GET':
            r = x_with_retries.get(url, timeout=300, **kwargs)
        elif req_type == 'POST':
            r = x.post(url, timeout=300, **kwargs)
        elif req_type == 'PUT':
            r = x.put(url, timeout=300, **kwargs)
        elif req_type == 'DELETE':
            r = x.delete(url, timeout=300, **kwargs)
        else:
            raise NotImplemented

        self._handle_error(r)
        return r

    def _api_call(self, req_type, path, data=None, params=None):
        response = self._api_call_raw(req_type, path, data=data, params=params)
        self._validate_json(response)
        return response.json()

    @staticmethod
    def _handle_error(response):
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
        
        if 'X-Qubole-Trace-Id' in response.headers:
            now = datetime.now()
            time = now.strftime('%Y-%m-%d %H:%M:%S')
            format_list = [time,response.headers['X-Qubole-Trace-Id']]
            sys.stderr.write("[{}] Request ID is: {}. Please share it with Qubole Support team for any assistance".format(*format_list) + "\n")
            
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
        elif code in (502, 503, 504):
            sys.stderr.write(response.text + "\n")
            raise RetryWithDelay(response)
        elif code == 449:
            sys.stderr.write(response.text + "\n")
            raise RetryWithDelay(response, "Data requested is unavailable. Retrying ...")
        elif 401 <= code < 500:
            sys.stderr.write(response.text + "\n")
            raise ClientError(response)
        elif 500 <= code < 600:
            sys.stderr.write(response.text + "\n")
            raise ServerError(response)
        else:
            raise ConnectionError(response)

    @staticmethod
    def _validate_json(response):
        # checks if the repose received is json decode-able
        try:
            response.json()
        except Exception as e:
            sys.stderr.write("Error: {0}\nInvalid Response from Server, please contact Qubole Support".format(str(e)))
            raise ServerError(response)
