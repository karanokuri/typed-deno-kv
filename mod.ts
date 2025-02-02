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

const assertEqualsType = <A, B>(
  _: [A] extends [B] ? [B] extends [A] ? true : false : false,
): void => {
};

Deno.test("Test utility types", () => {
  type TestSchema = {
    Key: ["preferences", string];
    Value: { username: string; theme: string; language: string };
  } | {
    Key: ["messages", string, string];
    Value: object;
  } | {
    Key: ["last_message_id"];
    Value: string;
  };

  assertEqualsType<
    TypedKvKey<TestSchema>,
    | ["preferences", string]
    | ["messages", string, string]
    | ["last_message_id"]
  >(true);

  assertEqualsType<
    TypedKvValue<TestSchema, ["preferences", string]>,
    { username: string; theme: string; language: string }
  >(true);

  assertEqualsType<
    TypedKvValue<TestSchema, ["messages", string, string]>,
    object
  >(true);

  assertEqualsType<
    TypedKvValue<TestSchema, ["last_message_id"]>,
    string
  >(true);

  assertEqualsType<
    TypedKvKeyPrefix<TestSchema>,
    | ["preferences"]
    | ["messages", string]
    | []
  >(true);

  assertEqualsType<
    TypedKvKeyPrefixed<TestSchema, []>,
    | ["preferences", string]
    | ["messages", string, string]
    | ["last_message_id"]
  >(true);

  assertEqualsType<
    TypedKvKeyPrefixed<TestSchema, ["preferences"]>,
    ["preferences", string]
  >(true);

  assertEqualsType<
    TypedKvEntry<TestSchema, ["preferences", string]>,
    {
      key: ["preferences", string];
      value: { username: string; theme: string; language: string };
      versionstamp: string;
    }
  >(true);

  assertEqualsType<
    TypedKvEntry<
      TestSchema,
      ["messages", string, string] | ["last_message_id"]
    >,
    {
      key: ["messages", string, string];
      value: object;
      versionstamp: string;
    } | {
      key: ["last_message_id"];
      value: string;
      versionstamp: string;
    }
  >(true);

  assertEqualsType<
    TypedKvEntry<
      TestSchema,
      ["messages", string, string] | ["last_message_id"]
    >,
    {
      key: ["messages", string, string] | ["last_message_id"];
      value: object | string;
      versionstamp: string;
    }
  >(false);

  assertEqualsType<
    TypedKvEntryMaybe<TestSchema, ["preferences", string]>,
    {
      key: ["preferences", string];
      value: { username: string; theme: string; language: string };
      versionstamp: string;
    } | {
      key: ["preferences", string];
      value: null;
      versionstamp: null;
    }
  >(true);

  assertEqualsType<
    TypedKvEntryMaybe<
      TestSchema,
      ["messages", string, string] | ["last_message_id"]
    >,
    {
      key: ["messages", string, string];
      value: object;
      versionstamp: string;
    } | {
      key: ["last_message_id"];
      value: string;
      versionstamp: string;
    } | {
      key: ["messages", string, string] | ["last_message_id"];
      value: null;
      versionstamp: null;
    }
  >(true);
});

Deno.test("Test TypedKv types", () => {
  type TestSchema = {
    Key: ["preferences", string];
    Value: { username: string; theme: string; language: string };
  } | {
    Key: ["messages", string, string];
    Value: object;
  } | {
    Key: ["last_message_id"];
    Value: string;
  };

  ((kv: TypedKv<TestSchema>) => {
    assertEqualsType<
      typeof kv.delete,
      (
        key:
          | ["preferences", string]
          | ["messages", string, string]
          | ["last_message_id"],
      ) => Promise<void>
    >(true);
  });

  (async (kv: TypedKv<TestSchema>) => {
    const key: ["preferences", string] = ["preferences", "alan"];
    const value = {
      username: "alan",
      theme: "light",
      language: "en-GB",
    };

    const result = await kv.atomic()
      .check({ key, versionstamp: null })
      .set(key, value)
      .commit();

    assertEqualsType<
      typeof result,
      Deno.KvCommitResult | Deno.KvCommitError
    >(true);
  });

  ((kv: TypedKv<TestSchema>) => {
    const result = kv.enqueue("foo", {
      keysIfUndelivered: [["last_message_id"]],
    });

    assertEqualsType<typeof result, Promise<Deno.KvCommitResult>>(true);
  });

  (async (kv: TypedKv<TestSchema>) => {
    const result = await kv.get(["preferences", "ada"]);

    assertEqualsType<
      typeof result,
      {
        key: ["preferences", string];
        value: { username: string; theme: string; language: string };
        versionstamp: string;
      } | {
        key: ["preferences", string];
        value: null;
        versionstamp: null;
      }
    >(true);
  });

  (async (kv: TypedKv<TestSchema>) => {
    const result = await kv.getMany([
      ["messages", "room1", "message1"],
      ["last_message_id"],
    ]);

    assertEqualsType<
      typeof result,
      [
        {
          key: ["messages", string, string];
          value: object;
          versionstamp: string;
        } | {
          key: ["messages", string, string];
          value: null;
          versionstamp: null;
        },
        {
          key: ["last_message_id"];
          value: string;
          versionstamp: string;
        } | {
          key: ["last_message_id"];
          value: null;
          versionstamp: null;
        },
      ]
    >(true);
  });

  ((kv: TypedKv<TestSchema>) => {
    const result = kv.list({ prefix: [] });

    assertEqualsType<
      typeof result,
      AsyncIterableIterator<
        {
          key: ["preferences", string];
          value: { username: string; theme: string; language: string };
          versionstamp: string;
        } | {
          key: ["messages", string, string];
          value: object;
          versionstamp: string;
        } | {
          key: ["last_message_id"];
          value: string;
          versionstamp: string;
        }
      > & { cursor: string }
    >(true);
  });

  ((kv: TypedKv<TestSchema>) => {
    kv.listenQueue((value) => {
      assertEqualsType<
        typeof value,
        | { username: string; theme: string; language: string }
        | object
        | string
      >(true);
    });
  });

  (async (kv: TypedKv<TestSchema>) => {
    const result = await kv.set(["preferences", "ada"], {
      username: "ada",
      theme: "dark",
      language: "en-US",
    });

    assertEqualsType<typeof result, Deno.KvCommitResult>(true);
  });

  ((kv: TypedKv<TestSchema>) => {
    const result = kv.watch([
      ["messages", "room1", "message1"],
      ["last_message_id"],
    ]);

    assertEqualsType<
      typeof result,
      ReadableStream<[
        {
          key: ["messages", string, string];
          value: object;
          versionstamp: string;
        } | {
          key: ["messages", string, string];
          value: null;
          versionstamp: null;
        },
        {
          key: ["last_message_id"];
          value: string;
          versionstamp: string;
        } | {
          key: ["last_message_id"];
          value: null;
          versionstamp: null;
        },
      ]>
    >(true);
  });
});
