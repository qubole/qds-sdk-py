from __future__ import print_function
import sys
import os
if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest

def print_command():
    print()
    for arg in sys.argv:
        print(arg, end=' ')
    print()


class QdsCliTestCase(unittest.TestCase):
    def setUp(self):
        os.environ['QDS_API_TOKEN'] = 'dummy_token'
