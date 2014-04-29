import sys
import os
import unittest2 as unittest
import contextlib
from cStringIO import StringIO


@contextlib.contextmanager
def capture():
    """
    This provides a context manager which captures stdout and stderr.
    """
    oldout, olderr = sys.stdout, sys.stderr
    try:
        out = [StringIO(), StringIO()]
        sys.stdout, sys.stderr = out
        yield out
    finally:
        sys.stdout, sys.stderr = oldout, olderr
        out[0] = out[0].getvalue()
        out[1] = out[1].getvalue()


def print_command():
    print
    for arg in sys.argv:
        print arg,
    print


class QdsCliTestCase(unittest.TestCase):
    def setUp(self):
        os.environ['QDS_API_TOKEN'] = 'dummy_token'
