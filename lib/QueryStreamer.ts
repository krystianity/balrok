import * as Debug from "debug";
const debug = Debug("balrok:streamer");

import { BalrokConfig } from "./types/BalrokConfig";
import Cache from "./Cache";

export default class QueryStreamer {

    private readonly config: BalrokConfig;
    private readonly cache: Cache;
    private parallelProcessCount: number;

    constructor(config: BalrokConfig, cache: Cache) {
        this.config = config;
        this.cache = cache;
        this.parallelProcessCount = 0;
    }

    public async runAndResolveQuery(model: any, query: any,
                                    documentOperation: (doc: any) => {keep: boolean, result: any},
                                    options: any = {}, batchSize: number = 512, order: number = -1,
                                    timeoutMs: number = 60 * 1000 * 3, dontAwait: boolean = false,
                                    noCache: boolean = false): Promise<any[] | { cacheKey: number }> {

        if (!model || typeof model.find !== "function") {
            throw new Error("Please pass a valid mongoose (schema) model: " + JSON.stringify(model));
        }

        if (!query || typeof query !== "object") {
            throw new Error("Please pass a valid query object: " + JSON.stringify(query));
        }

        if (!options || typeof options !== "object") {
            throw new Error("Please pass a valid options object: " + JSON.stringify(options));
        }

        const combinedQueryDescription = {...query, ...options, ...{ order }};
        const cacheKey = this.cache.getCacheKeyForQuery(combinedQueryDescription);
        const cacheEntry = await this.cache.getCacheState(cacheKey);

        // serve straight from cache and extend entry livetime
        if (cacheEntry && !cacheEntry.inProgress && !noCache) {

            if (dontAwait) {
                debug("Will not return cache entry, resolving asap", cacheKey);
                return { cacheKey };
            }

            debug("Serving query straight away from cached result", cacheKey);
            await this.cache.increaseCacheTime(cacheKey);
            return cacheEntry.queryResult;
        }

        // a process is already resolving this query, wait for that to happen
        if (cacheEntry && cacheEntry.inProgress) {

            if (dontAwait) {
                debug("Will not await, therefore resolving asap", cacheKey);
                return { cacheKey };
            }

            debug("Another process is already working on this query, awaiting his result", cacheKey);
            return await this.pollUntilNotInProgressAnymore(cacheKey, timeoutMs);
        }

        // no process is resolving the query and its result is not cached, start a new streaming process

        debug("Query result not stored in cache and no other processing working on it, starting new process",
            cacheKey, this.parallelProcessCount);

        if (this.config.maxParallelProcesses && this.parallelProcessCount >= this.config.maxParallelProcesses) {
            throw new Error("Max parallel process count reached, please wait for other queries to finish " + cacheKey);
        }

        await this.cache.setCacheState(cacheKey, true, null);

        this.parallelProcessCount++;
        if (!dontAwait) {
            return this.
                streamQuery(model, query, documentOperation, options, batchSize, order, timeoutMs)
                .then(async (result) => {
                    await this.cache.setCacheState(cacheKey, false, result);
                    this.parallelProcessCount--;
                    return result;
                }).catch(async (error) => {
                    await this.cache.deleteCacheState(cacheKey);
                    this.parallelProcessCount--;
                    throw error;
                });
        }

        // dont await, so run the process and resolve promise immediately
        this.
            streamQuery(model, query, documentOperation, options, batchSize, order, timeoutMs)
            .then(async (result) => {
                await this.cache.setCacheState(cacheKey, false, result);
                this.parallelProcessCount--;
            }).catch(async (error) => {
                await this.cache.deleteCacheState(cacheKey);
                this.parallelProcessCount--;
                debug("Error on unawaited streaming process", error.message);
            });

        return { cacheKey };
    }

    private pollUntilNotInProgressAnymore(cacheKey: number, timeoutMs: number = 60 * 1000 * 3): Promise<any[]> {
        return new Promise((resolve, reject) => {
            this.recursivePolling(cacheKey, (error, result) => {
                if (error) {
                    reject(error);
                } else {
                    resolve(result);
                }
            }, timeoutMs / 1000, 0);
        });
    }

    private recursivePolling(cacheKey: number, callback: (error: Error | null, result?: any) => void,
                             maxPoll: number = 30, pollCount: number = 0) {
        this.cache.getProcessedCacheState(cacheKey).then((result) => {
            if (result) {
                callback(null, result.queryResult);
            } else {
                pollCount++;
                if (pollCount > maxPoll) {
                    callback(new Error("Max poll count reached " + maxPoll));
                } else {
                    setTimeout(() => {
                        this.recursivePolling(cacheKey, callback, maxPoll, pollCount);
                    }, 1000);
                }
            }
        }).catch((error) => {
            callback(error);
        });
    }

    private streamQuery(model: any, query: any,
                        documentOperation: (doc: any) => {keep: boolean, result: any},
                        options: any, batchSize: number, order: number, timeoutMs: number): Promise<any[]> {
        return new Promise((resolve, reject) => {

            const startT = Date.now();
            let documentCount = 0;

            const stream = model
                .find(query, null, options)
                .sort({ _id: order })
                .lean()
                .batchSize(batchSize)
                .stream();

            const timeout = setTimeout(() => {
                debug("Streaming query timeout reached", timeoutMs, "destroying stream.");
                stream.destroy();
            }, timeoutMs);

            const results: any[] = [];
            stream
                .on("data", (doc: any) => {
                    documentCount++;
                    try {
                        const {keep, result} = documentOperation(doc);
                        if (keep) {
                            results.push(result);
                        }
                    } catch (error) {
                        debug("Error during execution of document operation", error.message);
                    }
                }).on("error", (error) => {
                    clearTimeout(timeout);
                    debug("Streaming query failed with error", error.message);
                    reject(error);
                }).on("close", () => {
                    clearTimeout(timeout);
                    const diff = Date.now() - startT;
                    debug("Resolved streaming query after", diff, "ms. Processed", documentCount,
                        "documents, on query:", query);
                    resolve(results);
                });
        });
    }
}
