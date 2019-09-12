LIB

 * Async / Await version for easier integration with async ecosystem.
    - how to cleanly stop?
    - how to detect deconnection and cleanly reconnect? (with pending scheduled callbacks and ongoing ones)
 * Graceful shutdown on signals (SIGNIT, SIGTERM…)
 * Integrate other aio libs HOWTO
 * Experiment with separate connections for consuming and producing

FOR PRODUCTION QUALITY:

* Add note about handling of refused or unroutable messages at application level
* More docstrings
* More docs
