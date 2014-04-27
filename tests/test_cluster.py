import sys
import os
import unittest
import contextlib
from mock import Mock
from cStringIO import StringIO
sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
from qds_sdk.connection import Connection


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


class TestCluster(unittest.TestCase):

    def test_list(self):
        sys.argv = ['qds.py', 'cluster', 'list']
        Connection._api_call = Mock(return_value=[])
        with capture() as captured:
            qds.main()
        #out = captured[0]
        #err = captured[1]
        Connection._api_call.assert_called_with("GET", "clusters", None)

    def test_list_id(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--id', '123']
        Connection._api_call = Mock(return_value=[])
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("GET", "clusters/123", None)

    def test_list_label(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--label', 'test_label']
        Connection._api_call = Mock(return_value=[])
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", None)

    def test_list_state_up(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'up']
        Connection._api_call = Mock(return_value=[])
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", None)

    def test_list_state_down(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'down']
        Connection._api_call = Mock(return_value=[])
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", None)

    def test_list_state_pending(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'pending']
        Connection._api_call = Mock(return_value=[])
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", None)

    def test_list_state_terminating(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'terminating']
        Connection._api_call = Mock(return_value=[])
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", None)

    def test_list_state_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'invalid']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_delete(self):
        sys.argv = ['qds.py', 'cluster', 'delete', '123']
        Connection._api_call = Mock(return_value={})
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("DELETE", "clusters/123", None)

    def test_delete_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'delete']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_delete_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'delete', '1', '2']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_start(self):
        sys.argv = ['qds.py', 'cluster', 'start', '123']
        Connection._api_call = Mock(return_value={})
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("PUT", "clusters/123/state",
                {'state': 'start'})

    def test_start_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'start']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_start_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'start', '1', '2']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_terminate(self):
        sys.argv = ['qds.py', 'cluster', 'terminate', '123']
        Connection._api_call = Mock(return_value={})
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("PUT", "clusters/123/state",
                {'state': 'terminate'})

    def test_terminate_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'terminate']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_terminate_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'terminate', '1', '2']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_status(self):
        sys.argv = ['qds.py', 'cluster', 'status', '123']
        Connection._api_call = Mock(return_value={})
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with("GET", "clusters/123/state",
                None)

    def test_status_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'status']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_status_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'status', '1', '2']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_reassign_label(self):
        sys.argv = ['qds.py', 'cluster', 'reassign_label', '123', 'test_label']
        Connection._api_call = Mock(return_value={})
        with capture() as captured:
            qds.main()
        Connection._api_call.assert_called_with('PUT',
                'clusters/reassign-label',
                {'destination_cluster': '123', 'label': 'test_label'})

    def test_reassign_label_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'reassign_label']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_reassign_label_less_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'reassign_label', '1']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()

    def test_reassign_label_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'reassign_label', '1', '2', '3']
        with self.assertRaises(SystemExit):
            with capture() as captured:
                qds.main()


if __name__ == '__main__':
    unittest.main()
