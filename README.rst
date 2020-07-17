==========
AsyncScope
==========

This library implements scoped taskgroups / nurseries.

Rationale
=========

Large programs often consist of building blocks which depend on each other.
Those dependencies may be non-trivial, aren't always linear, and generally
form some sort of directed acyclic graph instead of a nice linear or
hierarchical set of relationships.

Let's invent an example.

Your main code runs some admin module, which requires a support library,
which connects to a database. Halfway through it encounters an error, thus
loads an error handler, which also uses the database.

Another part of your program also needs the error handler database
connections and loading a handler are expensive, so you want to re-use
them.

Later the admin code terminates. It can't unload the error handler because
that other code still needs it.

This is a problem because you like to use Structured Programming
principles. Thus you need to jump through interesting hoops getting all of
this connected up and keeping track of each module's users.

Worse, assume that your code dies with a fatal exception. The exception
typically propagates through your code and cancels the database connection
before the error handler has a chance to log the problem. This happens
randomly, depending on which cancelled task runs first, so you have a lot
of fun trying to reproduce the problem and debug all of this.

AsyncScope can help you.

AsyncScope keeps track of your program's building blocks. It remembers
which parts depend on which other parts, prevents cyclic dependencies,
and terminates a scope as soon as nobody uses it any more.

Now your error handler stays around exactly as long as you need it, your
database connection won't die while the error handler (or any other code,
for that matter) requires it, your error gets logged correctly, and you
find the problem easily.

Usage
=====

Wrap your main code in ``async with main_scope(): ...``.

Start a service task (i.e. something you depend on) with ``srv = await
service(name, some_service, *params)``. ``srv`` is whatever object the service
intends you to use.

The service's setup code typically looks like this::

   from asyncscope import scope

   async def some_service(*params):
      s = await start_service(*params)
      await scope.register(s)
      await scope.no_more_dependents()
      s.close_service_cleanly()

Alternately, if the service is created by an async context manager::

   async def some_service(*params):
      async with service_context(*params) as s:
         await scope.register(s)
         await scope.no_more_dependents()

Every scope has a separate taskgroup which you can access by calling
``spawn()``. This function returns the new tasks's cancel scope so that you
can cancel the new task if you need to. All tasks started that way are
auto-cancelled when your main code exits.

Your service **must** call ``scope.register()`` exactly once,
otherwise the scopes waiting for it to start will wait forever. (They'll
get cancelled if your scope's main task exits before doing so.)

The current scope is available as the ``scope`` context variable.

The ``examples`` directory contains some sample code.

Cancellation semantics
======================

When a scope exits (either cleanly or when it raises an error that escapes
its taskgroup), all scopes depending on it are cancelled immediately, in
parallel. Then those it itself depends on are terminated cleanly and
in-order.

This also happens when a scope's main task ends.

"Clean termination" means that the scope's call to ``no_more_dependents()``
returns. If there is no such call, the scope's tasks are cancelled.

TODO: write a service which your code can use to keep another service alive
for a bit.

Code structure
==============

A scope's main code typically looks like this:

* setup: start other services, 

* call "register(serice_object)"

* ``await no_more_dependents()`` (subordinate task) or wait for SIGTERM (daemon main task)
  or terminate (main task's job is done)

* cleanly stop itself

If ``no_more_dependents`` is not used, the scope will be cancelled.

Scopes typically don't need to access its own scope object. It's stored in
a contextvar and can be retrieved via ``scope.get()`` if you need it.
For most uses, however, ``asyncscope``'s global ``scope`` object accesses
the current scope transparently.

