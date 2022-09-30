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

Your server contains some admin module, which requires a support library,
which connects to a database. Halfway through it encounters an error, thus
loads an error handler, which also uses the database.

Next, a client of your server does some shady stuff so you want to log
that, and since loading the error handler and connecting to the database is
expensive you want to re-use the handler you already have.

Later the admin code terminates. However, it shouldn't unload the error
handler, because that other code still needs it.

This is a problem because you like to use Structured Programming
principles, which state that if you started it you need to stop it.
Thus, you need to jump through some hoops getting all of this connected up,
keeping track of each module's users, and shutting things down in the
correct order.

Worse: assume that your code dies with a fatal exception. That exception
typically propagates through all of your code and thus tends to cancel the
database connection before the error handler has a chance to log the
problem. Worse, if the error from the logger occurs in a ``finally:`` block
it basically replaces the original exception, so you'll have a lot of fun
trying to debug all of this.

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

Add wrapper code to your services. A wrapper typically looks like this::

   from asyncscope import scope

   async def some_service(*p, **kw):
      srv = await start_your_service(*p, **kw)
      await scope.register(srv)
      await scope.no_more_dependents()
      srv.close_cleanly()

Alternately, if the service is created by an async context manager::

   async def some_service(*p, **kw):
      async with your_service_context(*p, **kw) as srv:
         await scope.register(srv)
         await scope.no_more_dependents()

Thus, given the above example, the code that runs your server would be wrapped
as a service. So would be the error handler, and the database code, and the
admin module.

Instead of using ``start_your_service`` or ``your_service_context``
functions to access a given service, all of these modules would do this::

    from asyncscope import service

    srv = await service(name, some_service, *params)

AsyncScope uses ``name`` to discover whether the service is already up and
running; if so, it remembers that the current scope is also using this
service and returns it. Usage cycles are forbidden.

Every service runs within a separate scope that's managed by `AsyncScope`
internally.

Note that ``service`` is **not** used as an async context manager. This is
intentional. It will be auto-closed when your service (or the main scope)
ends.

Every scope has a separate taskgroup which you can access by calling
``scope.spawn()``. This function returns a ``CancelScope`` that wraps the
new tasks, so that you can cancel it if you need to. All tasks started this
way are also auto-cancelled when the scope exits.

Your ``some_service`` code **must** call ``scope.register()`` exactly once,
otherwise the scopes waiting for it to start will wait forever. (They'll
get cancelled if your scope's main task exits before doing so.)

The current scope is available as the ``scope`` context variable.

The ``examples`` directory contains some sample code.


Cancellation semantics
======================

When a scope exits (either cleanly or when it raises an error that escapes
its taskgroup), the scopes depending on it are cancelled immediately, in
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

* do whatever you need to start the service. This code may start other
  scopes it depends on. Note that if the scope is already running,
  ``service`` simply returns its existing service object.

* call "register(serice_object)"

* ``await no_more_dependents()`` (subordinate task) or wait for SIGTERM (daemon main task)
  or terminate (main task's job is done)

* cleanly stop your service.

If ``no_more_dependents`` is not used, your code will be cancelled instead.

Scopes typically don't need to access their own scope object. It's stored in
a contextvar and can be retrieved via ``scope.get()`` if you need it.
For most uses, however, ``asyncscope``'s global ``scope`` object accesses
the current scope transparently.

If you want to release a service early, you can use ``async with scope.subscope():``
to create a new embeeded scope. Services started within this subscope are
auto-released when it exits.

Scopes and subscopes also afford the same interface as a taskgroup;
however, exiting the subscope cancels any still-running tasks instead of
waiting for them.

