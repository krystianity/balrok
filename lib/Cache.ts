import * as Debug from "debug";
const debug = Debug("balrok:cache");

import * as murmur from "murmurhash";
import * as moment from "moment";
import * as mongoose from "mongoose";
const Schema = mongoose.Schema;

import { BalrokConfig } from "./types/BalrokConfig";
import { CacheState } from "./types/CacheState";

export default class Cache {

    private readonly config: BalrokConfig;
    private model: any;

    constructor(config: BalrokConfig) {
        this.config = config;
    }

    public init() {

        const schemaDefinition = {
            cacheKey: Number, // hashed
            queryResult: Schema.Types.Mixed,
            inProgress: Boolean,
            deleteAt: Date,
        };

        const schema = new Schema(schemaDefinition);

        schema.index({ cacheKey: 1, type: -1});

        // ttl index
        schema.index({ deleteAt: 1 }, { expireAfterSeconds: 0 });

        this.model = mongoose.model(this.config.cacheCollectionName || "balrok_cache", schema);

        this.model.on("index", (error: Error) => {

            if (error) {
                debug("Index creation failed", error.message);
            } else {
                debug("Index creation successfull.");
            }
        });
    }

    public increaseCacheTime(cacheKey: number) {
        return this.model.findOneAndUpdate({ cacheKey }, {
            deleteAt: this.getTimeToExpire(),
        }, { upsert: false }).exec();
    }

    public getCacheState(cacheKey: number): Promise<CacheState | null> {
        return this.model.findOne({ cacheKey }).exec();
    }

    public getCacheStateForQuery(query: any): Promise<CacheState | null> {
        return this.model.findOne({
            cacheKey: this.getCacheKeyForQuery(query),
        }).exec();
    }

    public getProcessedCacheState(cacheKey: number): Promise<CacheState | null> {
        return this.model.findOne({ cacheKey, inProgress: false }).exec();
    }

    public setCacheState(cacheKey: number, inProgress: boolean = true, queryResult: any = null) {
        return this.model.findOneAndUpdate({ cacheKey }, {
            inProgress,
            queryResult,
            deleteAt: this.getTimeToExpire(),
        }, { upsert: true }).exec();
    }

    public deleteCacheState(cacheKey: number) {
        return this.model.deleteOne({ cacheKey }).exec();
    }

    public getCacheKeyForQuery(query: any): number {
        return this.hash(Object.keys(query).join(""));
    }

    private hash(value: string): number {
        return murmur.v3(value, 0);
    }

    private getTimeToExpire(): Date {
        return moment()
            .add(this.config.cacheTimeMs || 60 * 1000 * 5, "milliseconds")
            .toDate();
    }
}
