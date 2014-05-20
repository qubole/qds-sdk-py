import sys
import os
import unittest2 as unittest


def print_command():
    print
    for arg in sys.argv:
        print arg,
    print


class QdsCliTestCase(unittest.TestCase):
    def setUp(self):
        os.environ['QDS_API_TOKEN'] = 'dummy_token'
