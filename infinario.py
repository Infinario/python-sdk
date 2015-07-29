#!/usr/bin/env python

from __future__ import print_function

__title__ = 'infinario'
__version__ = '1.0.0'
__author__ = 'Peter Dolak'
__licence__ = 'Apache 2.0'
__copyright__ = 'Copyright 2015 7Segments s r.o.'


import json
import threading
import re
import requests
from requests.exceptions import ConnectionError
import logging
import time


# Python 2/3 compatibility fix
try:
    basestring = basestring
except NameError:
    basestring = (str, bytes)


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


class NullTransport(object):
    """
    NullTransport will make no requests.

    It is useful for disabling tracking in the Infinario constructor.
    """

    def __init__(self, *_args, **_kwargs):
        pass

    def send_and_receive(self, url, message):
        pass

    def send_and_ignore(self, url, message):
        pass


class SynchronousTransport(object):
    """
    SynchronousTransport is a simple synchronous transport using request.Session.

    Infinario methods identify, track, update and get_html will block for the whole time of a request.
    """

    def __init__(self, target, session=None, logger=None):
        self._logger = logger
        self._target = target
        self._session = session or requests.Session()

    def _send(self, service, message, params={}, no_raise=False):
        try:
            response = self._session.post(
                '{0}{1}'.format(self._target, service),
                data=json.dumps(message),
                params=params,
                headers={'Content-type': 'application/json'}
            )
        except ConnectionError as e:
            if no_raise:
                return self._logger.exception('Failed connecting to Infinario API at the given target URL {0}'
                                              .format(self._target))
            else:
                raise e

        if response.status_code == 401:
            if no_raise:
                return self._logger.exception('Infinario API authentication failed')
            else:
                raise AuthenticationError(response.text)

        json_response = response.json()

        if json_response.get('success', False):
            return json_response

        if response.status_code == 500:
            if no_raise:
                return self._logger.exception('Infinario API is not available or encountered an unknown error')
            else:
                raise ServiceUnavailable()

        errors = json_response.get('errors', [])
        if no_raise:
            return self._logger.exception('Infinario API request failed with errors: {0}'.format(str(errors)))
        else:
            raise InvalidRequest(errors)

    def send_and_receive(self, service, message, params={}):
        return self._send(service, message, params, no_raise=False)  # always non-silent, as the result is used

    def send_and_ignore(self, url, message):
        self._send(url, message, no_raise=bool(self._logger))


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

    def __init__(self, target, session=None, logger=None):
        # any variables used by more than one thread shall be here
        self._worker_data = _WorkerData(
            logger=logger,
            transport=SynchronousTransport(target=target, session=session, logger=logger),
            buffer=[],
            cv=threading.Condition(threading.Lock()),
            flush=False,
            stop=False
        )
        self._worker_running = False

    def send_and_receive(self, service, message, params={}):
        return self._worker_data.transport.send_and_receive(service, message, params)

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
                indices = range(len(data.buffer))
                message = {'commands': data.buffer[:ASYNC_BUFFER_MAX_SIZE]}

                data.cv.release()

                leftovers, errors = [], []
                results = data.transport.send_and_receive('bulk', message)['results']

                data.cv.acquire()

                for i in indices:
                    command = data.buffer[i]
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
                    if data.logger:
                        data.logger.exception(message)
                    else:
                        raise ServiceUnavailable(message)  # die after the first exception

                data.buffer = leftovers

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


class _InfinarioBase(object):
    def __init__(self, target=None, logger=None):
        if target:
            match = re.match('^(?:(https?:)?//)?([^/]+)(/*)$', target)
            if not match:
                if logger:
                    logger.error('Invalid Infinario target URL {0}'.format(target))
                else:
                    raise ValueError('Invalid target URL {0}'.format(target))
            self._target = '{0}//{1}/'.format(match.group(1) or 'https:', match.group(2))
        else:
            self._target = DEFAULT_TARGET


class AuthenticatedInfinario(_InfinarioBase):
    """
    Authenticated Infinario API access for exporting analysis data.
    """

    def __init__(self, username, password, target=None):
        """
        :param username: Username for an Infinario account with ExtAPI access
        :param password: Password for the account above
        :param target: Tracking API URL
        """
        super(AuthenticatedInfinario, self).__init__(target, None)
        session = requests.Session()
        session.auth = (username, password)
        self._transport = SynchronousTransport(target=self._target, session=session, logger=None)

    def export_analysis(self, analysis_type, data, token=None):
        """
        Compute and obtain data from an existing Infinario analysis definition

        :param analysis_type: funnel/report/retention/segmentation
        :param data: See http://guides.infinario.com/technical-guide/export-api/
        :param token: In case the Infinario account has access to multiple projects, specify the project token
        """
        params = {} if token is None else {'project': token}
        return self._transport.send_and_receive(
            'analytics/{0}'.format(analysis_type),
            data, params
        )


class Infinario(_InfinarioBase):
    """
    Infinario API access for tracking events, updating customer data and requesting campaign data.
    """

    def __init__(self, token, customer=None, target=None, silent=True, logger=None, transport=SynchronousTransport):
        """
        :param token: Project token to track data into.
        :param customer: Optional identifier of tracked customer (can later be done with method `identify`).
        :param target: Tracking API URL
        :param silent: True - non-fatal errors will be logged, False - exceptions will be thrown
        :param logger: Instance of a logger used with silent == True
        :param transport: One of `NullTransport`, `SynchronousTransport`, `AsynchronousTransport`;
            consult their documentation as well
        """
        logger = logger or DEFAULT_LOGGER if silent else None
        super(Infinario, self).__init__(target, logger)
        self._token = token
        self._customer = self._convert_customer_argument(customer)
        self._logger = logger
        self._transport = transport(target=self._target, logger=self._logger)

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
