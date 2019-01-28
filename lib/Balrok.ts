import * as Debug from "debug";
const debug = Debug("balrok:main");

import * as mongoose from "mongoose";

import { BalrokConfig } from "./types/BalrokConfig";
import Cache from "./Cache";
import QueryStreamer from "./QueryStreamer";
import { ResolveOptions } from "./types/ResolveOptions";

export default class Balrok {

    private readonly config: BalrokConfig;
    private readonly cache: Cache;
    private readonly queryStreamer: QueryStreamer;

    constructor(config: BalrokConfig) {
        this.config = config;
        this.cache = new Cache(config);
        this.queryStreamer = new QueryStreamer(config, this.cache);
    }

    public async init() {

        debug("Init..");

        if (!this.validatedMongooseConnectionState()) {
            const errorMessage = "Mongoose connection state is invalid. Balrok cannot create a connection"
            + " you have to to that beforehand.";
            debug(errorMessage);
            throw new Error(errorMessage);
        }

        this.cache.init();

        // async in case we ever have to extend any db operation here, to prevent breaking the api

        debug("Init done.");
    }

    public resolve(model: any, query: any,
                   documentOperation: (doc: any) => {keep: boolean, result: any}, resolveOptions: ResolveOptions):
                   Promise<any[] | { cacheKey: number }> {

                    const {
                        options = {},
                        batchSize = 512,
                        order = -1,
                        timeoutMs = 60 * 1000 * 3,
                        dontAwait = false,
                        noCache = false,
                    } = resolveOptions;

                    return this.queryStreamer.runAndResolveQuery(
                        model,
                        query,
                        documentOperation,
                        options,
                        batchSize,
                        order,
                        timeoutMs,
                        dontAwait,
                        noCache,
                    );
    }

    public resolveFlat(model: any, query: any,
                       documentOperation: (doc: any) => {keep: boolean, result: any},
                       options: any = {}, batchSize: number = 512, order: number = -1,
                       timeoutMs: number = 60 * 1000 * 3, dontAwait: boolean = false, noCache: boolean = false):
                   Promise<any[] | { cacheKey: number }> {
        return this.queryStreamer.runAndResolveQuery(
            model,
            query,
            documentOperation,
            options,
            batchSize,
            order,
            timeoutMs,
            dontAwait,
            noCache,
        );
    }

    public getRunningQueries() {
        return this.queryStreamer.getRunningQueries();
    }

    public abortRunningQuery(cacheKey: number) {
        return this.queryStreamer.abortQuery(cacheKey);
    }

    public getCacheKeyResult(cacheKey: number): Promise<any[] | null> {
        return this.cache.getProcessedCacheState(cacheKey).then((state) => {
            if (state) {
                return state.queryResult;
            } else {
                return null;
            }
        });
    }

    public deleteCacheKeyResult(cacheKey: number) {
        return this.cache.deleteCacheState(cacheKey);
    }

    private validatedMongooseConnectionState() {
        debug("Mongoose connection state", mongoose.connection.readyState);
        if (!mongoose.connection || !mongoose.connection.readyState) {
            return false;
        } else {
            return true;
        }
    }
}
