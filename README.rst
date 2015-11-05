Infinario Python SDK
====================

.. image:: https://travis-ci.org/Infinario/python-sdk.svg
    :target: https://travis-ci.org/Infinario/python-sdk
    :alt: Travis CI

The `infinario.Infinario` class provides access to the Infinario Python tracking API,
supporting both synchronous and asynchronous modes.

You can install the SDK using `pip`::

    pip install infinario

In order to track events, instantiate the class at least with your project token
(can be found in Project Management in your Infinario account), for example:

.. code-block:: python

    from infinario import Infinario

    client = Infinario('12345678-90ab-cdef-1234-567890abcdef')                  # PRODUCTION ENVIRONMENT
    # client = Infinario('12345678-90ab-cdef-1234-567890abcdef', silent=False)  # DEVELOPMENT ENVIRONMENT

We recommend to set the `silent` parameter to `False` in a development environment, as it will cause the Infinario API
to throw exceptions if something goes wrong. When left to the default value `True`, all errors will be logged
(also see the `logger` parameter).


Identifying the customer
------------------------

When tracking events, you have to specify which customer generated
them. This can be either done right when calling the client's
constructor.

.. code-block:: python

    python
    client = Infinario('12345678-90ab-cdef-1234-567890abcdef', customer='john123')

or by calling `identify`.

.. code-block:: python

    client.identify('john123')

Tracking events
---------------

To track events for the currently selected customer, simply
call the `track` method.

.. code-block:: python

    client.track('purchase')

You can also specify a dictionary of event properties to store
with the event.

.. code-block:: python

    client.track('purchase', {'product': 'bottle', 'amount': 5})

Specify the POSIX timestamp of the event using:

.. code-block:: python

    timestamp = time.time()

    # .. time passes ..

    client.track('purchase', timestamp=timestamp)

Updating customer properties
----------------------------

You can also update information that is stored with a customer.

.. code-block:: python

    client.update({'first_name': 'John', 'last_name': 'Smith'})

Getting HTML from campaign
--------------------------

.. code-block:: python

    client.get_html('Banner left')

will return::

    '<img src="/my-awesome-banner-1.png" />'

Transport types
---------------

By default the client uses a simple non-buffered synchronous transport. The three available transport types are:
* `NullTransport` - No requests, useful for disabling tracking in the Infinario constructor.
* `SynchronousTransport` - Most operations are blocking for the time of a request to the Infinario API
* `AsynchronousTransport` - Most operations are non-blocking (see the code for more information),
    buffered and using a single worker thread. Infinario client must be closed when no more data is to be tracked.

Example of choosing a transport:

.. code-block:: python

    from infinario import Infinario, AsynchronousTransport

    client = Infinario('12345678-90ab-cdef-1234-567890abcdef',
                       transport=AsynchronousTransport)

    # ...

    client.close()

Using on the command line
-------------------------

The python client also has a command-line interface that allows to call its essential functions:

.. code-block:: sh

    TOKEN='12345678-90ab-cdef-1234-567890abcdef'
    CUSTOMER='john123'

    # Track event
    ./infinario.py track "$TOKEN" "$CUSTOMER" purchase --properties product=bottle amount=5

    # Update customer properties
    ./infinario.py update "$TOKEN" "$CUSTOMER" first_name=John last_name=Smith

    # Get HTML from campaign
    ./infinario.py get_html "$TOKEN" "$CUSTOMER" "Banner left"

Infinario Python Authenticated API client
=========================================

The `infinario.AuthenticatedInfinario` class provides access to the Infinario
synchronous Python authenticated API. In order to export analyses you have to instantiate client
with username and password of user that has ExtAPI access:

.. code-block:: python

    from infinario import AuthenticatedInfinario

    client = AuthenticatedInfinario('username', 'password')

Exporting analyses
------------------

First argument is type of analysis (funnel, report, retention, segmentation),
second argument is JSON. In case that authenticated customer has access to multiple companies use keyword argument
`token=token_of_company_with_given_analysis`

.. code-block:: python

    client.export_analysis('funnel', {
        'analysis_id': '2f86608f-24f5-11e3-9950-c48508494cf5'
    })

will return

.. code-block:: python

    {
        "success": true,
        "name": "Conversion funnel",
        "steps": ["First visit", "Registration", "First log in", "Purchase", "Payment"],
        "total": {
            "counts": [48632, 24120, 20398, 1256, 1250],
            "times": [-1, 680, 4502, 45, 540, 300],
            "metric": 1987562
        },
        "drill_down": {
            "type": "none",
            "series": []
        },
        "metric": {
            "step": 4,
            "property": "price"
        }
    }
