# Balrok

# What?

Balrok is a mongoose helper for heavy lifting of un-indexed collections.

# Why?

Large MongoDB collections work very well, if you have enough RAM and
the fields you are filtering for are indexed. However that is very often not the case
and that might result in a lot of frustration, besides frying your cluster or getting
no results for minutes of query time.

Balrok helps you very easily to run your aggregations and filters using your client
in a safe and efficient manner. It ships with a distributed query processing stack,
as well as its own cache collection for your query result.

# How ?

You can simply spin up your mongoose setup as you are used to, as soon as you are connected to
MongoDB you create a new instance of Balrok and call `init` to prepare it. When you are ready to
go, you can pass your large un-indexed queries straight to your Balrok instance's `resolve` method.
Balrock will stream your documents from batch find queries and call your document operation function
on every document, the results are stored in the cache collection and shared with all instances. If another
instance of your code is already running the same query, balrok will await the results of that instances
process before returning them.

# Example

```javascript
    const balrok = new Balrok({
        cacheCollectionName: "balrok_cache_test",
        cacheTimeMs: 60 * 1000 * 5,
        maxParallelProcesses: 5,
    });

    await balrok.init();

    const query = {
        firstName: {
            $regex: "Chris",
        },
    };

    const documentOperation = (doc) => {
        return {
            keep: true,
            result: doc.surName,
        };
    };

    const resolveOptions = {
        options: {}, // mongoose find options
        batchSize: 12, // default is 512
        order: -1, // default is -1
        timeoutMs: 5000, // default is 3 minutes
        dontAwait: true, // default is false
        noCache: false, // default is false
    };

    const results = await balrok.resolve(testModel, query, documentOperation, resolveOptions));
```

A full running sample (with mongoose connection) can be found here `test/example.ts`.