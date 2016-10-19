cdmi-cli
========

An interactive command line client for CDMI.

Installation
------------

To install cdmi-cli from source:

.. code-block:: bash

    $ sudo python setup.py install

or alternatively (using virtualenv):

.. code-block:: bash

    (env) $ python setup.py install

Getting Started
---------------

To use (with caution), simply do:

.. code-block:: bash

    $ cdmi-cli
    cdmi @> ?
    available commands:
    quit   - quit client
    qos    - manage QoS for CDMI object
    help   - show help for available commands
    auth   - authentication to the CDMI server
    exit   - exit client
    query  - make CDMI query
    close  - close connection to CDMI server
    open   - open connection to CDMI server
    ?      - show available commands

More Examples
-------------

Simple Query
^^^^^^^^^^^^

To perform a simple query you need to open a connection first and provide some
authentication, for example:

.. code-block:: bash

    $ cdmi-cli
    cdmi @> open cdmi.exmpale.com 443
    cdmi @cdmi.example.com> auth basic
    Enter username: cdmi_user
    Enter password:
    cdmi @cdmi.example.com> query /

