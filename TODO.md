LIB

 * Failure handling / redelivers
    - sometimes it's better to let the service crash — and not reconnect! — than letting it messing with the messages (better let them in the queue until the service is fixed).
 * Documentation
 * Experiment with separate connections for consuming and producing
 * Async / Await version for easier integration with async ecosystem.

FOR PRODUCTION QUALITY:

* Add handling of refused or unroutable messages
* More docstrings
* More docs
