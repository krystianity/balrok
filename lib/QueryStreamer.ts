import * as Debug from "debug";
const debug = Debug("balrok:streamer");

import { BalrokConfig } from "./types/BalrokConfig";
import Cache from "./Cache";
import { QueryProgress } from "./types/QueryProgress";
import moment = require("moment");

export default class QueryStreamer {

    private readonly config: BalrokConfig;
    private readonly cache: Cache;
    private readonly queries: QueryProgress[];

    constructor(config: BalrokConfig, cache: Cache) {
        this.config = config;
        this.cache = cache;
        this.queries = [];
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
            cacheKey, this.queries.length);

        if (this.config.maxParallelProcesses && this.queries.length >= this.config.maxParallelProcesses) {
            throw new Error("Max parallel process count reached, please wait for other queries to finish " + cacheKey);
        }

        await this.cache.setCacheState(cacheKey, true, null);

        this.addQueryProgress(cacheKey, query, timeoutMs);
        if (!dontAwait) {
            return this.
                streamQuery(cacheKey, model, query, documentOperation, options, batchSize, order, timeoutMs)
                .then(async (result) => {
                    await this.cache.setCacheState(cacheKey, false, result);
                    this.removeQueryProgress(cacheKey);
                    return result;
                }).catch(async (error) => {
                    await this.cache.deleteCacheState(cacheKey);
                    this.removeQueryProgress(cacheKey);
                    throw error;
                });
        }

        // dont await, so run the process and resolve promise immediately
        this.
            streamQuery(cacheKey, model, query, documentOperation, options, batchSize, order, timeoutMs)
            .then(async (result) => {
                await this.cache.setCacheState(cacheKey, false, result);
                this.removeQueryProgress(cacheKey);
            }).catch(async (error) => {
                await this.cache.deleteCacheState(cacheKey);
                this.removeQueryProgress(cacheKey);
                debug("Error on unawaited streaming process", error.message);
            });

        return { cacheKey };
    }

    public getRunningQueries() {
        return this.queries;
    }

    public abortQuery(cacheKey: number): boolean {
        const queryProgress = this.getQueryProgress(cacheKey);
        if (queryProgress) {
            queryProgress.inAbortion = true;
            return true;
        } else {
            return false;
        }
    }

    private getQueryProgress(cacheKey: number): QueryProgress | null {

        for (const queryProgress of this.queries) {
            if (queryProgress.cacheKey === cacheKey) {
                return queryProgress;
            }
        }

        debug("Did not find query progress for ", cacheKey);
        return null;
    }

    private addQueryProgress(cacheKey: number, query: any, timeoutMs: number): number {
        return this.queries.push({
            cacheKey,
            query,
            startedAt: moment().toDate(),
            timesoutAt: moment().add(timeoutMs, "milliseconds").toDate(),
            processedDocuments: 0,
            colectedResults: 0,
            documentErrors: 0,
            inAbortion: false,
        });
    }

    private removeQueryProgress(cacheKey: number): boolean {

        for (let i = this.queries.length - 1; i >= 0; i--) {
            if (this.queries[i].cacheKey === cacheKey) {
                debug("Removing query progress", this.queries[i]);
                this.queries.splice(i, 1);
                return true;
            }
        }

        debug("Did not find query progress for", cacheKey, "failed to remove it.");
        return false;
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

    private streamQuery(cacheKey: number, model: any, query: any,
                        documentOperation: (doc: any) => {keep: boolean, result: any},
                        options: any, batchSize: number, order: number, timeoutMs: number): Promise<any[]> {
        return new Promise((resolve, reject) => {

            const startT = Date.now();
            const results: any[] = [];
            let documentCount = 0;
            let errorCount = 0;
            let shouldAbortOnNextData = false;

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

            const intv = setInterval(() => {
                const queryProgress = this.getQueryProgress(cacheKey);
                if (queryProgress) {
                    queryProgress.processedDocuments = documentCount;
                    queryProgress.colectedResults = results.length;
                    queryProgress.documentErrors = errorCount;
                    shouldAbortOnNextData = queryProgress.inAbortion;
                }
            }, 5);

            // timeouts and aborts do not throw errors, because the collected
            // results until the point might be of interest

            stream
                .on("data", (doc: any) => {

                    if (shouldAbortOnNextData) {
                        debug("Query streaming aborted as requested by user", cacheKey);
                        stream.destroy();
                        return;
                    }

                    documentCount++;
                    try {
                        const {keep, result} = documentOperation(doc);
                        if (keep) {
                            results.push(result);
                        }
                    } catch (error) {
                        errorCount++;
                        debug("Error during execution of document operation", error.message);
                    }
                }).on("error", (error) => {
                    clearInterval(intv);
                    clearTimeout(timeout);
                    debug("Streaming query failed with error", error.message);
                    reject(error);
                }).on("close", () => {
                    clearInterval(intv);
                    clearTimeout(timeout);
                    const diff = Date.now() - startT;
                    debug("Resolved streaming query after", diff, "ms. Processed", documentCount,
                        "documents, on query:", cacheKey, query);
                    resolve(results);
                });
        });
    }
}
