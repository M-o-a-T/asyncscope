==========
AsyncScope
==========

This library implements scoped taskgroups / nurseries.


Rationale
=========

Composability
+++++++++++++

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

Worse: let's say that your code dies with a fatal exception. That exception
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
database connection won't die while the error handler (or any other code)
requires it, your error gets logged correctly, and you find the problem.


Multiple services
+++++++++++++++++

Some programs need any number of async contexts, e.g. a client that talks
to any number of servers within the same method. Creating server contexts
is often awkward under these circumstances; you need a subtask or an
`contextlib.AsyncExitStack` to act as the contexts' "keeper". However,
setting up a subtask is a lot of boilerplate; an exit stack doesn't help
when you need to dynamically remove servers.

`AsyncScope` helps by decoupling code structure from service usage, while
ensuring that external connections and other subtasks are not duplicated
*and* ended cleanly when their last user terminates.


Usage
=====

Main code
+++++++++

Wrap your main code in an ``async with asyncscope.main_scope(): ...`` block.

This call initializes the global `AsyncScope.scope` object. It always
refers to the current service (i.e. initially, your main code).


Wrapping services
+++++++++++++++++

A "service" is defined as any nontrivial object that might be used from
multiple contexts within your program. That can be a HTTP session, or a
database connection, any object that's non-trivial to create, …

`AsyncScope` requires wrapping your service in a function with common
calling conventions because it doesn't know (indeed doesn't want to know)
the details of how to set up and tear down your service objects.

Here are some examples.

If the service uses a ``run`` method, you'd do this::

   from asyncscope import scope

   async def some_service(*p, **kw):
      srv = your_service(*p, **kw)
      async with anyio.create_task_group() as tg:
         await tg.start(srv.run)

         scope.register(srv)
         await scope.no_more_dependents()

         await srv.stop_running()
         tg.cancel_scope.cancel()

Alternately, if the service runs as an async context manager::

   from asyncscope import scope

   async def some_service(*p, **kw):
      async with your_service(*p, **kw) as srv:
         # NB: some services use "async with await …"
         scope.register(srv)
         await scope.no_more_dependents()

Alternately², it might run as a context-free background service::

   from asyncscope import scope

   async def some_service(*p, **kw):
      srv = your_service(*p, **kw)
      srv = await srv.start()

      scope.register(srv)
      await scope.no_more_dependents()

      await srv.aclose()

Alternately³, if the service is an annoying-to-set-up object::

   from asyncscope import scope

   async def some_service(*p, **kw):
      srv = SomeObject(*p, **kw)
      await SomeObject.costly_setup()

      scope.register(srv)
      try:
         await scope.no_more_dependents()
      finally:
         srv.teardown()
      # use this to e.g. clean up circular references within your object


Next, we'll see how to use these objects.


Using services
++++++++++++++

Using `AsyncScope`, a service is used in one of two ways.

* within a context::

    from asyncscope import scope

    async with scope.using_service(name, some_service, *p, **kw) as srv:
       ...

* until the caller's scope ends *or* you explicitly release it::

    from asyncscope import scope

    srv = await scope.service(name, some_service, *p, **kw)
    ...
    del srv  # don't hog the memory!
    scope.release(name)

 * check whether a named service exists::

    from asyncscope import scope

    try:
        srv = scope.lookup(name)
    except KeyError:
       pass  # no it does not
    else:
       ...
       del srv
       scope.release(name)

In all three cases ``srv`` is the object that your ``some_service`` code has
passed to `AsyncScope.Scope.register`.

.. note::

    `Scope.lookup` raises `KeyError` if the scope is currently being
    set up. The other methods wait for the service's call to `Scope.register`.


Service naming
++++++++++++++

AsyncScope uses ``name`` to discover whether the service is already up and
running. If so, it records that the current scope is also using this named
service and simply returns it.

Names must be globally unique. To avoid collisions, add your object class,
an idenifier like ``id(YourServiceClass)``, or ``id(container_object)``
to it, depending on usage.

`AsyncScope` does not try to derive uniqueness from its parameters, because
arbitrary naming conventions are unlikely to work for everybody. One easy
way to disambiguate potential collisions is to include
``id(some_service)`` in the name.

Implications
++++++++++++

Calling `Scope.service` or `Scope.using_service` does not guarantee that
the service in question will start when you do: it might have been running
already. Likewise, leaving the ``async with`` block or exiting the caller's
scope may not stop the service: there might be other users, or some caching
mechanism that delays closing it.

Calling these functions twice / nesting `Scope.using_service` calls is OK.
Usage cycles (service A starts service B which later requires A) are
forbidden and will be detected.

Every scope contains a taskgroup which you can access using the usual
``start`` and ``start_soon`` methods. You can also call ``scope.spawn()``.
This function returns a ``CancelScope`` that wraps the new tasks, so you
can cancel it if you need to. All tasks started this way are also
auto-cancelled when the scope exits.

Your ``some_service`` code **must** call ``scope.register()`` exactly once,
otherwise the scopes waiting for it to start will wait forever. (They'll
get cancelled if your scope's main task exits before doing so.)

The current scope is available as the ``scope`` context variable.

The ``examples`` directory contains some sample code.


Exception handling
==================

This section describes the effects of an exception that escapes from a
service's main task, causing it to terminate.

Errors that are subclasses of `BaseException` but not `Exception` are
never caught. If the service did not yet call `Scope.register` they may
receive either a `concurrent.Futures.CancelledError`, or a cancellation
exception from the async framework.

`Exception`\s raised after the service called `Scope.register` are not
handled. They will ultimately propagate out of the `AsyncScope.main_scope`
block.

Otherwise the error are propagated to the caller(s) that are waiting
for its `Scope.register` call.

Otherwise the exception is left unhandled; the effects are described in the
nest section.

Cancellation semantics
======================

When a scope exits (either cleanly or when it raises an error that escapes
its taskgroup), the scopes depending on it are cancelled immediately, in
parallel. Then, those it itself depends on are terminated cleanly and
in-order, assuming they're not used by some other scope.

This also happens when a scope's main task ends.

"Clean termination" means that the scope's call to ``no_more_dependents()``
returns. If there is no such call open, the scope's tasks are cancelled.


TODO: write a service which your code can use to keep another service alive
for a bit.


Code structure
==============

A scope's main code typically looks like this:

* do whatever you need to start the service. This code may start other
  scopes it depends on. Note that if the scope is already running,
  ``service`` simply returns its existing service object.

* call ``scope.register(serice_object)``

* call ``await scope.no_more_dependents()`` (subordinate task) or wait for SIGTERM (daemon main task)
  or terminate (main task's job is done)

* cleanly stop your service.

If ``no_more_dependents`` is not used, your code will be cancelled instead.

Scopes typically don't need to access their own scope object. It's stored in
a contextvar and can be retrieved via ``scope.get()`` if you need it.
For most uses, however, ``asyncscope``'s global ``scope`` object accesses
the current scope transparently.

Temporary services
++++++++++++++++++

Some services don't need to be running all the time. To release a service
early, use ``async with scope.subscope():``. This creates an embeeded scope.
Services started within this subscope are auto-released when it exits,
assuming as usual that no other code uses them.

When a (sub)scope's main task ends, any still-running tasks running within
its task group are cancelled instead of waiting for them to end (as a
"normal" task group would).

