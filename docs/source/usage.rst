Usage
=====

Once you've installed the Watchful SDK and have a running Watchful application instance (and optionally, a running Hub instance), you're ready to use the SDK.

Connecting to the Watchful App
------------------------------

If you're creating hinters in an existing project, you'd need to connect to your already-running Watchful application instance and its currently active project.

.. automethod:: watchful.client.external

Example Implementation
......................

    .. code-block:: python

        import watchful as w

        # Connect to your hosted Watchful application instance
        host = "your.watchful.application.host"  # change this string to your actual host
        port = "9001"

        w.external(host, port)

We can verify the connection by calling ``w.get()``. After you've connected to your hosted Watchful application instance, call this function any time you want to check on it's status. If everything works, you'll see the `ProjectSummary` returned.

If you're not using the UI to manage your projects, and want to do everything using the API, you can create a new project with ``w.create_project()``. It will additionally be opened automatically, so you don't need to call ``w.open_project(...)``. You can give it a title with ``w.title("My Project")``. We will show this in a later section below.

We recommend using the UI and the API at the same time, so you can connect to your hosted Watchful application via the API in a notebook or from plain Python, and at the same time also visualize your work in the UI.

A Walk-through of the API
-------------------------

Now that you've connected to a running Watchful application instance, let's take a look at the key parts of the API that are most commonly used.

.. automethod:: watchful.client.get

You can use ``w.get()`` at any time to get the current status. The structure returned is called the summary, and it is fairly complete. We've already used ``w.get()`` earlier as a quick check after connecting to your Watchful application instance.

It's worth noting that:

* The frontend of your Watchful application gets everything that it displays from this same summary object, so if you see it in the frontend, you can find it in the summary.
* The summary object is returned from every API call, not just ``w.get()``, so if you call any oher function that sends a request to your Watchful application, you'd always be returned with the summary object.
* The fields of the summary object will always be there.

It's also worth mentioning a couple of these fields that are especially useful:

* The ``status`` field tells you whether the backend is doing work or not, and as we can see here it is "current", which is usually what you want. If it is "working", then the backend is still doing some work, and you can expect that some things may change. An example is creating a hinter, as we'll do below, when you can see that the ``summary`` object returns immediately with a status of "working", and the hinter is still in the progress of being fully applied to all the candidates in the background, at which point it will go back to "current".

* The ``error_msg`` field reports the error information if there is any error. If there is a value in this field, it means the API request did not succeed, so check this field when appropriate.

Let's get the list of projects in your Watchful application instance.

.. automethod:: watchful.client.list_projects

This will return something like this::

    [
        {
            path: "/root/watchful/projects/1664235417.be7863f8-008d-4f0d-9993-2f16d39a3c24",
            shared: True,
            title: "Untitled Project 2022-09-22",
        },
        {
            path: "/root/watchful/2022-09-28.hints",
            shared: False,
            title: "Untitled Project 2022-09-28",
        },
        ...
    ]

For further examples, see the `examples directory <https://github.com/Watchfulio/watchful-py/tree/main/examples>`_ in the Github Repository.
