export interface CacheState {
    cacheKey: number;
    queryResult: any;
    inProgress: boolean;
    deleteAt: Date;
}
