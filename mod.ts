export type Schema = {
  Key: Deno.KvKey;
  Value: unknown;
};

export type Key<T extends Schema> = T["Key"];

export type Value<
  T extends Schema,
  K extends Key<T>,
> = T extends Schema ? K extends T["Key"] ? T["Value"] : never
  : never;

export type KeyU64<T extends Schema> = Key<T> extends infer K
  ? K extends Key<T> ? Value<T, K> extends Deno.KvU64 ? K : never : never
  : never;

export type KeyPrefix<
  T extends Schema,
> = Key<T> extends [...infer Prefix, Deno.KvKeyPart] ? Prefix : [];

export type KeyPrefixed<
  T extends Schema,
  P extends KeyPrefix<T>,
> = Key<T> extends infer K
  ? K extends Key<T> & [...P, ...Deno.KvKey] ? K : never
  : never;

export type ListSelector<
  T extends Schema,
  P extends KeyPrefix<T>,
> =
  & Deno.KvListSelector
  & (
    | { prefix: P }
    | { prefix: P; start: KeyPrefixed<T, P> }
    | { prefix: P; end: KeyPrefixed<T, P> }
    | { start: KeyPrefixed<T, P>; end: KeyPrefixed<T, P> }
  );

export type Entry<
  T extends Schema,
  K extends Key<T>,
> = K extends Key<T> ? {
    key: K;
    value: Value<T, K>;
    versionstamp: string;
  }
  : never;

export type EntryMaybe<
  T extends Schema,
  K extends Key<T>,
> =
  | Entry<T, K>
  | {
    key: K;
    value: null;
    versionstamp: null;
  };

export type ListIterator<
  T extends Schema,
  P extends KeyPrefix<T>,
> = AsyncIterableIterator<Entry<T, KeyPrefixed<T, P>>> & {
  get cursor(): string;
};

export class AtomicOperation<T extends Schema> {
  private readonly op: Deno.AtomicOperation;

  constructor(op: Deno.AtomicOperation) {
    this.op = op;
  }

  check(...checks: { key: Key<T>; versionstamp: string | null }[]): this {
    this.op.check(...checks);
    return this;
  }

  commit(): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    return this.op.commit();
  }

  delete(key: Key<T>): this {
    this.op.delete(key);
    return this;
  }

  enqueue<U extends readonly Key<T>[]>(
    value: Value<T, U[number]>,
    options?: {
      delay?: number;
      keysIfUndelivered?: [...U];
      backoffSchedule?: number[];
    },
  ): this {
    this.op.enqueue(value, options);
    return this;
  }

  max(key: KeyU64<T>, n: bigint): this {
    this.op.max(key, n);
    return this;
  }

  min(key: KeyU64<T>, n: bigint): this {
    this.op.min(key, n);
    return this;
  }

  mutate<K extends Key<T>>(
    ...mutations: (
      & { key: K }
      & (
        | { type: "set"; value: Value<T, K>; expireIn?: number }
        | { type: "delete" }
        | (Value<T, K> extends Deno.KvU64 ? {
            type: "sum";
            value: Deno.KvU64;
          }
          : never)
        | (Value<T, K> extends Deno.KvU64 ? {
            type: "max";
            value: Deno.KvU64;
          }
          : never)
        | (Value<T, K> extends Deno.KvU64 ? {
            type: "min";
            value: Deno.KvU64;
          }
          : never)
      )
    )[]
  ): this {
    this.op.mutate(...mutations);
    return this;
  }

  set<K extends Key<T>>(
    key: K,
    value: Value<T, K>,
    options?: { expireIn?: number },
  ): this {
    this.op.set(key, value, options);
    return this;
  }

  sum(key: Key<T>, n: bigint): this {
    this.op.sum(key, n);
    return this;
  }
}

export class Kv<T extends Schema> {
  private readonly kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.kv = kv;
  }

  [Symbol.dispose]() {
    this.kv[Symbol.dispose]();
  }

  atomic(): AtomicOperation<T> {
    return new AtomicOperation(this.kv.atomic());
  }

  close(): void {
    this.kv.close();
  }

  commitVersionstamp(): symbol {
    return this.kv.commitVersionstamp();
  }

  delete(key: Key<T>): Promise<void> {
    return this.kv.delete(key);
  }

  enqueue<U extends readonly Key<T>[]>(
    value: Value<T, U[number]>,
    options?: {
      delay?: number;
      keysIfUndelivered?: [...U];
      backoffSchedule?: number[];
    },
  ): Promise<Deno.KvCommitResult> {
    return this.kv.enqueue(value, options);
  }

  get<K extends Key<T>>(
    key: K,
    options?: { consistency?: Deno.KvConsistencyLevel },
  ): Promise<EntryMaybe<T, K>> {
    return this.kv.get(key, options) as Promise<EntryMaybe<T, K>>;
  }

  getMany<U extends readonly Key<T>[]>(
    keys: readonly [...U],
    options?: { consistency?: Deno.KvConsistencyLevel },
  ): Promise<{ [K in keyof U]: EntryMaybe<T, U[K]> }> {
    return this.kv.getMany(keys, options) as Promise<
      { [K in keyof U]: EntryMaybe<T, U[K]> }
    >;
  }

  list<P extends KeyPrefix<T>>(
    selector: ListSelector<T, P>,
    options?: Deno.KvListOptions,
  ): ListIterator<T, P> {
    return this.kv.list(selector, options) as ListIterator<T, P>;
  }

  listenQueue(
    handler: (value: Value<T, Key<T>>) => Promise<void> | void,
  ): Promise<void> {
    return this.kv.listenQueue(handler);
  }

  set<K extends Key<T>>(
    key: K,
    value: Value<T, K>,
    options?: { expireIn?: number },
  ): Promise<Deno.KvCommitResult> {
    return this.kv.set(key, value, options);
  }

  watch<U extends readonly Key<T>[]>(
    keys: readonly [...U],
    options?: { raw?: boolean },
  ): ReadableStream<{ [K in keyof U]: EntryMaybe<T, U[K]> }> {
    return this.kv.watch(keys, options) as ReadableStream<
      { [K in keyof U]: EntryMaybe<T, U[K]> }
    >;
  }
}
