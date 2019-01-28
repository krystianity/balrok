export interface QueryProgress {
    cacheKey: number;
    operationName: string;
    operationType: string;
    query: any;
    startedAt: Date;
    timesoutAt: Date;
    processedDocuments: number;
    colectedResults: number;
    documentErrors: number;
    inAbortion: boolean;
}
