import * as TypedKv from "./mod.ts";

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
    TypedKv.Key<TestSchema>,
    | ["preferences", string]
    | ["messages", string, string]
    | ["last_message_id"]
  >(true);

  assertEqualsType<
    TypedKv.Value<TestSchema, ["preferences", string]>,
    { username: string; theme: string; language: string }
  >(true);

  assertEqualsType<
    TypedKv.Value<TestSchema, ["messages", string, string]>,
    object
  >(true);

  assertEqualsType<
    TypedKv.Value<TestSchema, ["last_message_id"]>,
    string
  >(true);

  assertEqualsType<
    TypedKv.KeyPrefix<TestSchema>,
    | ["preferences"]
    | ["messages", string]
    | []
  >(true);

  assertEqualsType<
    TypedKv.KeyPrefixed<TestSchema, []>,
    | ["preferences", string]
    | ["messages", string, string]
    | ["last_message_id"]
  >(true);

  assertEqualsType<
    TypedKv.KeyPrefixed<TestSchema, ["preferences"]>,
    ["preferences", string]
  >(true);

  assertEqualsType<
    TypedKv.Entry<TestSchema, ["preferences", string]>,
    {
      key: ["preferences", string];
      value: { username: string; theme: string; language: string };
      versionstamp: string;
    }
  >(true);

  assertEqualsType<
    TypedKv.Entry<
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
    TypedKv.Entry<
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
    TypedKv.EntryMaybe<TestSchema, ["preferences", string]>,
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
    TypedKv.EntryMaybe<
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

Deno.test("Test TypedKv. types", () => {
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

  ((kv: TypedKv.Kv<TestSchema>) => {
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

  (async (kv: TypedKv.Kv<TestSchema>) => {
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

  ((kv: TypedKv.Kv<TestSchema>) => {
    const result = kv.enqueue("foo", {
      keysIfUndelivered: [["last_message_id"]],
    });

    assertEqualsType<typeof result, Promise<Deno.KvCommitResult>>(true);
  });

  (async (kv: TypedKv.Kv<TestSchema>) => {
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

  (async (kv: TypedKv.Kv<TestSchema>) => {
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

  ((kv: TypedKv.Kv<TestSchema>) => {
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

  ((kv: TypedKv.Kv<TestSchema>) => {
    kv.listenQueue((value) => {
      assertEqualsType<
        typeof value,
        | { username: string; theme: string; language: string }
        | object
        | string
      >(true);
    });
  });

  (async (kv: TypedKv.Kv<TestSchema>) => {
    const result = await kv.set(["preferences", "ada"], {
      username: "ada",
      theme: "dark",
      language: "en-US",
    });

    assertEqualsType<typeof result, Deno.KvCommitResult>(true);
  });

  ((kv: TypedKv.Kv<TestSchema>) => {
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
