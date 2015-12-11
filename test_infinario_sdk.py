import json
import sys
if sys.version_info < (3, ):
    import unittest2 as unittest
else:
    import unittest
import re
import requests
from infinario import Infinario, SynchronousTransport
try:
    from mock import MagicMock, patch
except ImportError:
    from unittest.mock import MagicMock, patch


class TestInfinarioSDK(unittest.TestCase):

    # helper for assertions about API requests
    def _pop_post_calls(self, mock):
        calls = []
        for call in [call for call in mock.mock_calls if call[0] == '().post']:
            match = re.match('^https://nope/(.*)$', call[1][0])
            self.assertTrue(match)
            self.assertEquals({'Content-type': 'application/json'}, call[2]['headers'])
            calls.append((match.group(1), json.loads(call[2]['data'])))
        mock.reset_mock()
        return calls

    # testing unidentified track, identify, identified track, update and identify at init with simple sync transport
    @patch.object(requests, 'Session')
    def test_synchronous_transport(self, session_mock):
        infinario = Infinario('t', target='//nope', transport=SynchronousTransport, secret='xyz')
        infinario.track('e1', {'prop1': 'val'})
        infinario.identify(customer='joe', properties={'prop2': 'val'})
        infinario.track('e2')
        infinario.update({'prop3': 'val'})

        expected = [
            ('crm/events', {'customer_ids': {}, 'project_id': 't', 'properties': {'prop1': 'val'}, 'type': 'e1'}),
            ('crm/customers', {'ids': {'registered': 'joe'}, 'project_id': 't', 'properties': {'prop2': 'val'}}),
            ('crm/events', {'customer_ids': {'registered': 'joe'}, 'project_id': 't', 'properties': {}, 'type': 'e2'}),
            ('crm/customers', {'ids': {'registered': 'joe'}, 'project_id': 't', 'properties': {'prop3': 'val'}})
        ]
        self.assertEquals(expected, self._pop_post_calls(session_mock))

        infinario = Infinario('u', customer='john', target='nope/')
        infinario.track('e1', {'prop1': 'val'})
        infinario.get_segment('123-456')

        expected = [
            ('crm/events',
             {'customer_ids': {'registered': 'john'}, 'project_id': 'u', 'properties': {'prop1': 'val'}, 'type': 'e1'}),
            ('analytics/segmentation-for',
             {'analysis_id': '123-456', 'customer_ids': {'registered': 'john'}, 'timeout': 0.5, 'timezone': 'UTC'})
        ]
        self.assertEquals(expected, self._pop_post_calls(session_mock))

    # asynchronous transport test removed as it is deprecated


class TestConvertTimestampArgument(unittest.TestCase):
    def test_int(self):
        self.assertEquals(Infinario._convert_timestamp_argument(10), 10)

    def test_float(self):
        self.assertEquals(Infinario._convert_timestamp_argument(10.1), 10.1)

    def test_with_timestamp_method(self):
        m = MagicMock()
        m.timestamp = MagicMock(return_value=123)

        self.assertEquals(Infinario._convert_timestamp_argument(m), 123)

        m.timestamp.assert_called_once_with()

    def test_other(self):
        with self.assertRaisesRegex(ValueError, 'Cannot convert \'a string\' to timestamp'):
            Infinario._convert_timestamp_argument('a string')
