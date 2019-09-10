API Documentation
=================

Continuation based API
----------------------

This module is for *callbacks* style of asynchronous application development.

We depend on the `pika` library for lower level communication and event loop.

.. autoclass:: async_service.base.Service
   :members: __init__, use_json, use_exclusive_queues, log, send_command, return_success, return_error, publish_event, call_later, run, stop, handle_event, handle_command, handle_result, handle_returned_message, on_ready
   :undoc-members:

.. autoclass:: async_service.supervisor.ReconnectingSupervisor
    :members: __init__, run
    :undoc-members:
