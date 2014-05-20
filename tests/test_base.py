import sys
import os

major, minor = sys.version_info.major, sys.version_info.minor
if (major >= 2 and minor >= 7):
    import unittest
else:
    import unittest2 as unittest

def print_command():
    print
    for arg in sys.argv:
        print arg,
    print


class QdsCliTestCase(unittest.TestCase):
    def setUp(self):
        os.environ['QDS_API_TOKEN'] = 'dummy_token'
