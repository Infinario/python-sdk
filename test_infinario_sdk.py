import json
import unittest
import re
import requests
import time
from infinario import Infinario, AuthenticatedInfinario, NullTransport, SynchronousTransport, AsynchronousTransport
try:
    from mock import MagicMock, patch
except ImportError:
    from unittest.mock import MagicMock, patch


def _fake_post_response(*args, **kwargs):
    mock = MagicMock()
    mock.status_code = 200
    mock.json = lambda: {'success': True, 'results': [{'status': 'ok'}]*20 + [{'status': 'retry'}]*100}
    return mock


class TestInfinarioSDK(unittest.TestCase):

    # helper for assertions about API requests
    def _pop_post_calls(self, mock):
        calls = []
        for call in [call for call in mock.mock_calls if call[0] == '().post']:
            match = re.match('^nope(.*)$', call[1][0])
            self.assertTrue(match)
            self.assertEquals({'Content-type': 'application/json'}, call[2]['headers'])
            calls.append((match.group(1), json.loads(call[2]['data'])))
        mock.reset_mock()
        return calls

    # helper for assertions about API bulk requests
    def _assert_bulk_events(self, mock, expected_leftovers, type):
        calls = self._pop_post_calls(mock)
        self.assertEquals(len(expected_leftovers), len(calls))
        for call, expected in zip(calls, expected_leftovers):
            self.assertEquals('/bulk', call[0])
            events = 0
            for command in call[1]:
                self.assertEquals('/crm/events', command['name'])
                self.assertEquals(type, command['data']['type'])
                events += 1
            self.assertEquals(expected, events)

    # testing unidentified track, identify, identified track, update and identify at init with simple sync transport
    @patch.object(requests, 'Session')
    def test_synchronous_transport(self, session_mock):
        infinario = Infinario('t', target='nope', transport=SynchronousTransport)
        infinario.track('e1', {'prop1': 'val'})
        infinario.identify(customer='joe', properties={'prop2': 'val'})
        infinario.track('e2')
        infinario.update({'prop3': 'val'})

        expected = [
            ('/crm/events', {'customer_ids': {}, 'project_id': 't', 'properties': {'prop1': 'val'}, 'type': 'e1'}),
            ('/crm/customers', {'ids': {'registered': 'joe'}, 'project_id': 't', 'properties': {'prop2': 'val'}}),
            ('/crm/events', {'customer_ids': {'registered': 'joe'}, 'project_id': 't', 'properties': {}, 'type': 'e2'}),
            ('/crm/customers', {'ids': {'registered': 'joe'}, 'project_id': 't', 'properties': {'prop3': 'val'}})
        ]
        self.assertEquals(expected, self._pop_post_calls(session_mock))

        infinario = Infinario('u', customer='john', target='nope')
        infinario.track('e1', {'prop1': 'val'})

        expected = [
            ('/crm/events',
             {'customer_ids': {'registered': 'john'}, 'project_id': 'u', 'properties': {'prop1': 'val'}, 'type': 'e1'})
        ]
        self.assertEquals(expected, self._pop_post_calls(session_mock))

    # testing async transport - flush, buffer fill, timeout and close
    @patch.object(requests, 'Session')
    def test_asynchronous_transport(self, session_mock):
        session_mock().post.side_effect = _fake_post_response

        # flush
        infinario = Infinario('t', target='nope', transport=AsynchronousTransport)
        infinario.track('e1')
        infinario.flush()

        time.sleep(0.2)
        self._assert_bulk_events(session_mock, [1], 'e1')

        # full buffer, will 2x push 20 through
        for _ in range(80):
            infinario.track('e2')

        time.sleep(0.2)
        self._assert_bulk_events(session_mock, [80, 60], 'e2')

        # timeout, again 2x 20
        time.sleep(1.1)
        self._assert_bulk_events(session_mock, [40, 20], 'e2')

        # close
        infinario.track('e3')
        infinario.close()

        time.sleep(0.2)
        self._assert_bulk_events(session_mock, [1], 'e3')