export interface QueryProgress {
    cacheKey: number;
    query: any;
    startedAt: Date;
    timesoutAt: Date;
    processedDocuments: number;
    colectedResults: number;
    documentErrors: number;
    inAbortion: boolean;
}
