=========================
SET SESSION AUTHORIZATION
=========================

Synopsis
--------

.. code-block:: none

    SET SESSION AUTHORIZATION username

Description
-----------

``SET SESSION AUTHORIZATION username`` sets the user to run as for the current session.
For the ``SET SESSION AUTHORIZATION username`` statement to succeed, the principal should be
able to impersonate the user connected as (X-Presto-User), and that user should be able to
impersonate the username. Presto will then allow the principal to authenticate for a query
that effectively runs as the username. Username can be inputted as either a string or an identifier.

Examples
--------

In the following example, the original user when the connection to Presto is made is Kevin.
The following sets the session authorization user to John::

    SET SESSION AUTHORIZATION 'John';

Queries will now execute as John instead of Kevin.
