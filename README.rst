==========
asyncscope
==========

This library implements scoped taskgroups / nurseries.

Rationale
=========

Large programs often consist of building blocks which depend on each other.
Crucially, these blocks may get started in FIFO order: you start the main
program which starts a mid-level support task which starts a low-level
connection. Then it stats another mid-level task which starts a low-level
task which uses the same connection … you get the idea: these relationships
typically form a DAG (directed acyclic graph).

In order to cleanly terminate this, the main program must be cancelled
first (so that it may clean up after itself), *then* the mid-level stuff
(data conversion, error reporting …) gets torn down, *then* the
low-level connection (database, MQTT, …).


Usage
=====

Wrap your main code in ``async with main_scope(): ...``.

Start a service task (i.e. something you depend on) with ``await
spawn_service(async_proc, ...)``. Service tasks are cancelled automatically
when your main code exits. Don't do it yourself, you'll create a deadlock.
if the service scope wants to create a new

Start some other subtask which runs in parallel with your main task 
with ``cancel_scope = await spawn(async_proc, ...)``. You can cancel
them like any other task if necessary.

The current scope is available as the ``scope`` context variable.

The ``examples`` directory has sample code.

Cancellation semantics
======================

When a scope exits (either cleanly or when it raises an error that escapes
its taskgroup), all scopes depending on it are cancelled immediately, in
parallel. Then those it itself depends on are terminated cleanly and
in-order.

This also happens when a scope's main task ends.

"Clean termination" means that the scope's task is cancelles unless it waits for
``no_more_dependents()`` to return.

Code structure
==============

A scope's main code typically looks like this:

* start the services it depends on

* call "register(serice_object)"

* ``await no_more_dependents()`` (subordinate task) or wait for SIGTERM (daemon main task)
  or terminate (main task's job is done)

* cleanly stop itself

If ``no_more_dependents`` is not called, the scope will be cancelled; as
this typically affects all subtasks, clean shutdown is unnecessarily
difficult.

Scopes typically don't need to access its own scope object. It's stored in
the ``asyncscope.scope`` contextvar if you need it. ``asyncscope``'s global
procedures access the current scope transparently.
