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

  check(
    ...checks: { key: Key<T>; versionstamp: string | null }[]
  ): AtomicOperation<T> {
    this.op.check(...checks);
    return this;
  }

  commit(): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    return this.op.commit();
  }

  set<K extends Key<T>>(
    key: K,
    value: Value<T, K>,
    options?: { expireIn?: number },
  ): AtomicOperation<T> {
    this.op.set(key, value, options);
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
