API Documentation
=================

Continuation based API
----------------------

This module is for *callbacks* style of asynchronous application development.

We depend on the `pika` library for lower level communication and event loop.

.. autoclass:: py_eda_tools.async_service.pika.Service
   :members: __init__, use_json, use_exclusive_queues, log, send_command, return_success, return_error, publish_event, call_later, run, stop, handle_event, handle_command, handle_result, handle_returned_message, on_ready, PREFETCH_COUNT, RETRY_DELAY

.. autoclass:: py_eda_tools.async_service.pika.ReconnectingSupervisor
    :members: __init__, run


Asyncio compatible API
----------------------

Depends on the aiormq library::

    pip install .[asyncio]


.. autoclass:: py_eda_tools.async_service.aio.Service
   :members: __init__, use_json, use_exclusive_queues, log, send_command, return_success, return_error, publish_event, handle_event, handle_command, handle_result, on_ready, create_task, stop, run, PREFETCH_COUNT, RETRY_DELAY


Synchronous publisher
---------------------

Depends on the kombu library::

    pip install .[synchronous]


.. autoclass:: py_eda_tools.sync_publisher.Endpoint
    :members: __init__, publish_event, log
