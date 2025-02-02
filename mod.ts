export type TypedKvSchema = {
  Key: Deno.KvKey;
  Value: unknown;
};

export type TypedKvKey<T extends TypedKvSchema> = T["Key"];

export type TypedKvValue<
  T extends TypedKvSchema,
  K extends TypedKvKey<T>,
> = T extends TypedKvSchema ? K extends T["Key"] ? T["Value"] : never
  : never;

export type TypedKvKeyPrefix<
  T extends TypedKvSchema,
> = TypedKvKey<T> extends [...infer Prefix, Deno.KvKeyPart] ? Prefix : [];

export type TypedKvKeyPrefixed<
  T extends TypedKvSchema,
  P extends TypedKvKeyPrefix<T>,
> = TypedKvKey<T> extends infer K
  ? K extends TypedKvKey<T> & [...P, ...Deno.KvKey] ? K : never
  : never;

export type TypedKvListSelector<
  T extends TypedKvSchema,
  P extends TypedKvKeyPrefix<T>,
> =
  & Deno.KvListSelector
  & (
    | { prefix: P }
    | { prefix: P; start: TypedKvKeyPrefixed<T, P> }
    | { prefix: P; end: TypedKvKeyPrefixed<T, P> }
    | { start: TypedKvKeyPrefixed<T, P>; end: TypedKvKeyPrefixed<T, P> }
  );

export type TypedKvEntry<
  T extends TypedKvSchema,
  K extends TypedKvKey<T>,
> = K extends TypedKvKey<T> ? {
    key: K;
    value: TypedKvValue<T, K>;
    versionstamp: string;
  }
  : never;

export type TypedKvEntryMaybe<
  T extends TypedKvSchema,
  K extends TypedKvKey<T>,
> =
  | TypedKvEntry<T, K>
  | {
    key: K;
    value: null;
    versionstamp: null;
  };

export type TypedKvListIterator<
  T extends TypedKvSchema,
  P extends TypedKvKeyPrefix<T>,
> = AsyncIterableIterator<TypedKvEntry<T, TypedKvKeyPrefixed<T, P>>> & {
  get cursor(): string;
};

export class TypedAtomicOperation<T extends TypedKvSchema> {
  private readonly op: Deno.AtomicOperation;

  constructor(op: Deno.AtomicOperation) {
    this.op = op;
  }

  check(
    ...checks: { key: TypedKvKey<T>; versionstamp: string | null }[]
  ): TypedAtomicOperation<T> {
    this.op.check(...checks);
    return this;
  }

  commit(): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    return this.op.commit();
  }

  set<K extends TypedKvKey<T>>(
    key: K,
    value: TypedKvValue<T, K>,
    options?: { expireIn?: number },
  ): TypedAtomicOperation<T> {
    this.op.set(key, value, options);
    return this;
  }
}

export class TypedKv<T extends TypedKvSchema> {
  private readonly kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.kv = kv;
  }

  [Symbol.dispose]() {
    this.kv[Symbol.dispose]();
  }

  atomic(): TypedAtomicOperation<T> {
    return new TypedAtomicOperation(this.kv.atomic());
  }

  close(): void {
    this.kv.close();
  }

  commitVersionstamp(): symbol {
    return this.kv.commitVersionstamp();
  }

  delete(key: TypedKvKey<T>): Promise<void> {
    return this.kv.delete(key);
  }

  enqueue<U extends readonly TypedKvKey<T>[]>(
    value: TypedKvValue<T, U[number]>,
    options?: {
      delay?: number;
      keysIfUndelivered?: [...U];
      backoffSchedule?: number[];
    },
  ): Promise<Deno.KvCommitResult> {
    return this.kv.enqueue(value, options);
  }

  get<K extends TypedKvKey<T>>(
    key: K,
    options?: { consistency?: Deno.KvConsistencyLevel },
  ): Promise<TypedKvEntryMaybe<T, K>> {
    return this.kv.get(key, options) as Promise<TypedKvEntryMaybe<T, K>>;
  }

  getMany<U extends readonly TypedKvKey<T>[]>(
    keys: readonly [...U],
    options?: { consistency?: Deno.KvConsistencyLevel },
  ): Promise<{ [K in keyof U]: TypedKvEntryMaybe<T, U[K]> }> {
    return this.kv.getMany(keys, options) as Promise<
      { [K in keyof U]: TypedKvEntryMaybe<T, U[K]> }
    >;
  }

  list<P extends TypedKvKeyPrefix<T>>(
    selector: TypedKvListSelector<T, P>,
    options?: Deno.KvListOptions,
  ): TypedKvListIterator<T, P> {
    return this.kv.list(selector, options) as TypedKvListIterator<T, P>;
  }

  listenQueue(
    handler: (value: TypedKvValue<T, TypedKvKey<T>>) => Promise<void> | void,
  ): Promise<void> {
    return this.kv.listenQueue(handler);
  }

  set<K extends TypedKvKey<T>>(
    key: K,
    value: TypedKvValue<T, K>,
    options?: { expireIn?: number },
  ): Promise<Deno.KvCommitResult> {
    return this.kv.set(key, value, options);
  }

  watch<U extends readonly TypedKvKey<T>[]>(
    keys: readonly [...U],
    options?: { raw?: boolean },
  ): ReadableStream<{ [K in keyof U]: TypedKvEntryMaybe<T, U[K]> }> {
    return this.kv.watch(keys, options) as ReadableStream<
      { [K in keyof U]: TypedKvEntryMaybe<T, U[K]> }
    >;
  }
}
