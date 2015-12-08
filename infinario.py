#!/usr/bin/env python

from __future__ import print_function
import json
import threading
import re
import requests
from requests.exceptions import ConnectionError, Timeout
import logging
import time


# Python 2/3 compatibility fixes
import sys

try:
    basestring = basestring
except NameError:
    basestring = (str, bytes)

if sys.version < '3':
    u = lambda string: unicode(string)
else:
    u = lambda string: string


DEFAULT_TARGET = 'https://api.infinario.com/'
DEFAULT_LOGGER = logging.getLogger(__name__)
ASYNC_BUFFER_MAX_SIZE = 50  # number of customer updates and events before flushing
ASYNC_BUFFER_TIMEOUT = 1  # max seconds before buffer is flushed


class InvalidRequest(Exception):
    pass


class ServiceUnavailable(Exception):
    pass


class AuthenticationError(Exception):
    pass


class ErrorHandler(object):
    def __init__(self, silent=False, logger=None):
        self._silent = silent
        self._logger = logger or DEFAULT_LOGGER

    def handle(self, error_message, exception_cls, no_raise=False):
        if self._silent or no_raise:
            self._logger.error(error_message)
            return
        else:
            raise exception_cls(error_message)


class NullTransport(object):
    """
    NullTransport will make no requests.

    It is useful for disabling tracking in the Infinario constructor.
    """

    def __init__(self, *_args, **_kwargs):
        pass

    def send_and_receive(self, url, message, no_raise=False, timeout=None):
        pass

    def send_and_ignore(self, url):
        pass


class SynchronousTransport(object):
    """
    SynchronousTransport is a simple synchronous transport using request.Session.

    Infinario methods identify, track, update and get_html will block for the whole time of a request.
    """

    def __init__(self, target, errors, session=None):
        self._errors = errors
        self._target = target
        self._session = session or requests.Session()

    def _send(self, service, message, no_raise=False, timeout=None):
        try:
            response = self._session.post(
                u('{0}{1}').format(self._target, service),
                data=json.dumps(message),
                headers={'Content-type': 'application/json'},
                timeout=timeout,
            )
        except ConnectionError:
            return self._errors.handle(
                u('Failed connecting to Infinario API at the given target URL {0}').format(self._target),
                ServiceUnavailable, no_raise=no_raise)
        except Timeout:
            return self._errors.handle(
                u('Infinario request to {0} failed to complete within timeout {1}').format(service, timeout),
                ServiceUnavailable, no_raise=no_raise)

        if response.status_code == 401:
            return self._errors.handle(
                u('Infinario API authentication failure'),
                AuthenticationError, no_raise=no_raise)

        json_response = response.json()

        if json_response.get('success', False):
            return json_response

        errors = json_response.get('errors', None) or response.text

        if response.status_code in (503, 504):
            return self._errors.handle(
                u('Infinario API is currently unavailable or under too much load: {0}').format(errors),
                ServiceUnavailable, no_raise=no_raise)

        return self._errors.handle(
            u('Infinario API request failed with errors: {0}').format(errors),
            InvalidRequest, no_raise=no_raise)

    def send_and_receive(self, service, message, no_raise=False, timeout=None):
        # always non-silent, as the result is used
        return self._send(service, message, no_raise=no_raise, timeout=timeout)

    def send_and_ignore(self, url, message):
        self._send(url, message)


class _WorkerData(object):
    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)


class AsynchronousTransport(object):
    """
    AsynchronousTransport is a buffered asynchronous transport using one lazy-initialized thread and requests.Session.
     This transport requires that the close method is called once the client will no longer be used.

    Infinario method get_html may block for the whole time of a request;
     methods identify, track, update, flush and close are non-blocking (consult class Infinario for more information).
    Asynchronous commands will be buffered up to ASYNC_BUFFER_MAX_SIZE of commands and will be flushed at most after
     ASYNC_BUFFER_TIMEOUT seconds.
    """

    def __init__(self, target, errors, session=None):
        # any variables used by more than one thread shall be here
        self._worker_data = _WorkerData(
            errors=errors,
            transport=SynchronousTransport(target, errors, session=session),
            buffer=[],
            cv=threading.Condition(threading.Lock()),
            flush=False,
            stop=False
        )
        self._worker_running = False

    def send_and_receive(self, service, message, no_raise=False, timeout=None):
        return self._worker_data.transport.send_and_receive(service, message, no_raise=no_raise, timeout=timeout)

    def send_and_ignore(self, service, message):
        command = {'name': service, 'data': message, 'scheduled': time.time()}
        self._ensure_lazy_worker()

        with self._worker_data.cv:
            self._worker_data.buffer.append(command)
            self._worker_data.cv.notify()

    def _ensure_lazy_worker(self):
        if self._worker_data.stop:
            raise ValueError('The API is already closed')
        if self._worker_running:
            return

        data = self._worker_data

        class Worker(threading.Thread):

            def run(self):
                data.cv.acquire()

                while True:
                    size = len(data.buffer)
                    timeout_in = data.buffer[0]['scheduled'] + ASYNC_BUFFER_TIMEOUT - time.time() if size > 0 else None
                    timeouted = timeout_in is not None and timeout_in < 0

                    if (size > 0 and data.flush) or size > ASYNC_BUFFER_MAX_SIZE or timeouted:
                        self._send_bulk()
                        if len(data.buffer) == 0:
                            data.flush = False
                    else:
                        if data.stop:
                            break
                        data.cv.wait(timeout_in)

                data.cv.release()

            def _send_bulk(self):
                selected = data.buffer[:ASYNC_BUFFER_MAX_SIZE]
                message = {'commands': selected}

                data.cv.release()

                leftovers, errors = [], []
                results = data.transport.send_and_receive('bulk', message)['results']

                data.cv.acquire()

                for i, command in enumerate(selected):
                    status = results[i].get('status', 'missing') if i < len(results) else 'retry'

                    if status == 'ok':
                        pass
                    elif status == 'retry':
                        leftovers.append(command)
                    else:
                        errors.append('Infinario API bulk command failed with status {0}, errors: {1}'.format(
                            status, str(results[i].get('errors', []))
                        ))

                for message in errors:
                    data.errors.handle(message, ServiceUnavailable)  # die after the first exception

                data.buffer = leftovers + data.buffer[ASYNC_BUFFER_MAX_SIZE:]

        Worker().start()
        self._worker_running = True

    def flush(self):
        with self._worker_data.cv:
            self._worker_data.flush = True
            self._worker_data.cv.notify()

    def stop(self):
        with self._worker_data.cv:
            self._worker_data.stop = True
            self._worker_data.flush = True
            self._worker_data.cv.notify()


class Infinario(object):
    """
    Infinario API access for tracking events, updating customer data and requesting campaign data.
    If the secret argument is passed, it also allows exporting analyses.
    """

    def __init__(self, token,
                 customer=None, target=None, silent=True, logger=None, transport=SynchronousTransport, secret=None):
        """
        :param token: Project token to track data into.
        :param customer: Optional identifier of tracked customer (can later be done with method `identify`).
        :param target: Tracking API URL
        :param silent: True - non-fatal errors will be logged, False - exceptions will be thrown
        :param logger: Instance of a logger used with silent == True
        :param transport: One of `NullTransport`, `SynchronousTransport`, `AsynchronousTransport`;
            consult their documentation as well
        :param secret: Secret token of a project with analyses that should be exported
        """
        errors = ErrorHandler(silent, logger)
        self._error_handler = errors
        if target:
            match = re.match('^(?:(https?:)?//)?([^/]+)(/*)$', target)
            if not match:
                errors.handle(u('Invalid Infinario target URL {0}').format(target), ValueError)
            self._target = '{0}//{1}/'.format(match.group(1) or 'https:', match.group(2))
        else:
            self._target = DEFAULT_TARGET
        self._token = token
        self._customer = self._convert_customer_argument(customer)
        session = requests.Session()
        if secret:
            session.headers.update({'X-Infinario-Secret': secret})
        self._transport = transport(self._target, errors, session=session)

    def identify(self, customer=None, properties=None):
        """
        Identify a customer, optionally update their properties.
        :param customer: Customer identifier
        :param properties: Optional dictionary of properties
        """
        self._customer = self._convert_customer_argument(customer)
        if properties is not None:
            self.update(properties)

    def update(self, properties):
        """
        Update the properties of the currently identified customer.
        :param properties: Dictionary of properties
        """
        self._transport.send_and_ignore('crm/customers', {
            'ids': self._customer,
            'project_id': self._token,
            'properties': properties
        })

    def track(self, event_type, properties=None, timestamp=None):
        """
        Track an event for the currently identified customer.
        :param event_type: Type of the event to track.
        :param properties: Optional dictionary of properties
        """
        data = {
            'customer_ids': self._customer,
            'project_id': self._token,
            'type': event_type,
            'properties': {} if properties is None else properties
        }
        if timestamp is not None:
            data['timestamp'] = self._convert_timestamp_argument(timestamp)
        self._transport.send_and_ignore('crm/events', data)

    def get_html(self, html_campaign_name):
        """
        Get the HTML code to display in case the customer is targeted in a HTML campaign action.
        :param html_campaign_name: Name of the campaign
        :return: HTML code to display
        """
        response = self._transport.send_and_receive('campaigns/html/get', {
            'customer_ids': self._customer,
            'project_id': self._token,
            'html_campaign_name': html_campaign_name
        })
        return response['data']

    def export_analysis(self, analysis_type, data):
        """
        Compute and the result of an existing analysis stored in Infinario in the project authenticated by secret

        :param analysis_type: funnel/report/retention/segmentation
        :param data: See http://guides.infinario.com/technical-guide/export-api/
        """
        return self._transport.send_and_receive('analytics/{0}'.format(analysis_type), data)

    def get_segment(self, segmentation_id, timezone='UTC', timeout=0.5):
        """
        Compute the result of a segmentation for the identified customer

        :param segmentation_id: id of the segmentation already stored in the Infinario project authenticated by secret
        :param timezone: optional, Olson TZ database string specifying the timezone, default UTC
        :param timeout: optional, number of seconds to wait for the result, otherwise return None, default 0.5 seconds
        :returns segment name string for the customer, None if could not be determined
        """
        try:
            result = self._transport.send_and_receive('analytics/segmentation-for', {
                'analysis_id': segmentation_id,
                'customer_ids': self._customer,
                'timezone': timezone,
                'timeout': timeout,
            }, no_raise=True, timeout=timeout)
        except ServiceUnavailable:
            return None

        if not result or not isinstance(result, dict):
            return None

        return result.get('segment', None)

    def flush(self):
        """
        If using asynchronous buffered transport, this will flush all tracked data to the API.
        """
        getattr(self._transport, 'flush', lambda: None)()

    def close(self):
        """
        If using asynchronous buffered transport, this method MUST be called when the client is no longer to be used.

        Aside from flushing the buffers, it will also destroy the worker thread.
        """
        getattr(self._transport, 'stop', lambda: None)()

    @staticmethod
    def _convert_customer_argument(customer):
        if customer is None:
            return {}
        elif isinstance(customer, basestring):
            return {'registered': customer}
        elif isinstance(customer, dict):
            return customer
        raise ValueError('Attribute customer should be None, string or dict')

    @staticmethod
    def _convert_timestamp_argument(timestamp):
        if timestamp is None:
            return None
        elif isinstance(timestamp, (int, float)):
            return timestamp
        elif hasattr(timestamp, 'timestamp'):
            return timestamp.timestamp()
        else:
            raise ValueError('Cannot convert {0!r} to timestamp'.format(timestamp))


def _add_common_arguments(parser):
    parser.add_argument('token')
    parser.add_argument('registered_customer_id')
    parser.add_argument('--target', default=DEFAULT_TARGET, metavar='URL')


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    commands = parser.add_subparsers()

    def property(s):
        prop = s.split('=', 1)
        if len(prop) != 2:
            raise ValueError('Property value not defined')
        return prop

    def track():
        client.track(args.event_type, dict(args.properties))

    def update():
        client.update(dict(args.properties))

    def get_html():
        print(client.get_html(args.html_campaign_name))

    parser_track = commands.add_parser('track', help='Track event')
    _add_common_arguments(parser_track)
    parser_track.add_argument('event_type')
    parser_track.add_argument('--properties', nargs='+', help='key=value', type=property, default=[])
    parser_track.set_defaults(func=track)

    parser_update = commands.add_parser('update', help='Update customer properties')
    _add_common_arguments(parser_update)
    parser_update.add_argument('properties', nargs='+', help='key=value', type=property, default=[])
    parser_update.set_defaults(func=update)

    parser_get_html = commands.add_parser('get_html', help='Get HTML from campaign')
    _add_common_arguments(parser_get_html)
    parser_get_html.add_argument('html_campaign_name')
    parser_get_html.set_defaults(func=get_html)

    args = parser.parse_args()

    client = Infinario(args.token, customer=args.registered_customer_id, target=args.target, silent=False)
    args.func()
