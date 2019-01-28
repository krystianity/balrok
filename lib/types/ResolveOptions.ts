export interface ResolveOptions {
    options?: any;
    batchSize?: number;
    order?: number;
    timeoutMs?: number;
    dontAwait?: boolean;
    noCache?: boolean;
    initialValue?: any;
    limit?: number | null;
}
