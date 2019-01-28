import * as Debug from "debug";
const debug = Debug("balrok:test");

import * as mongoose from "mongoose";
const Schema = mongoose.Schema;

import * as assert from "assert";

import Balrok from "../lib/Balrok";

const testSchemaDefinition = {
    firstName: String,
    surName: String,
};

const testSchema = new Schema(testSchemaDefinition);
const testModel = mongoose.model("balrok_test", testSchema);

mongoose.set("bufferCommands", false);
mongoose.set("useCreateIndex", true);
mongoose.connect("mongodb://localhost:27017/balrok", Object.assign({}, {
    autoReconnect: true,
    useNewUrlParser: true,
    noDelay: true,
    keepAlive: true,
    reconnectTries: 30,
    reconnectInterval: 1000,
    poolSize: 10,
} as any));

mongoose.connection.once("open", async () => {

    await testModel.create({
        firstName: "Chanti",
        surName: "Chris",
    });

    await testModel.create({
        firstName: "Chris",
        surName: "Chanti",
    });

    await (new Promise((resolve) => setTimeout(resolve, 500)));

    /* ### example code starts here ### */

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

    const documentOperation = (doc: any) => {
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

    const { cacheKey } =
        (await balrok.resolve(testModel, query, documentOperation, resolveOptions)) as { cacheKey: number };
    debug(cacheKey);

    assert.ok(balrok.getRunningQueries().length);

    await (new Promise((resolve) => setTimeout(resolve, 500)));

    const results = await balrok.getCacheKeyResult(cacheKey);
    await balrok.deleteCacheKeyResult(cacheKey);

    assert.ok(!balrok.getRunningQueries().length);

    // you can also await the result directly:

    const directResults =
        (await balrok.resolve(testModel, query, documentOperation,
            {...resolveOptions, ...{ dontAwait: false }})) as any[];

    debug(results!.length, results);
    debug(directResults.length, directResults);

    // without clean up, results of next test run will be served from cache
    await balrok.deleteCacheKeyResult(cacheKey);

    /* ### example code ends here ### */

    assert.ok(results);
    assert.ok(directResults.length);
    assert.equal(results!.length, directResults.length);

    mongoose.connection.close();
});
