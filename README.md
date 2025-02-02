# Typed Deno KV

Type-safe Deno.Kv wrapper.

## Usage

```typescript
import { TypedKv } from "https://raw.githubusercontent.com/karanokuri/typed-deno-kv/main/mod.ts";

type UserId = string;
type User = {
  username: string;
  theme: string;
  language: string;
};
type RoomId = string;
type MessageId = string;
type Message = object;

// Opening a database
using kv = new TypedKv<
  {
    Key: ["preferences", UserId];
    Value: User;
  } | {
    Key: ["messages", RoomId, MessageId];
    Value: Message;
  } | {
    Key: ["last_message_id"];
    Value: MessageId;
  }
>(await Deno.openKv(":memory:"));

{
  const prefs = {
    username: "ada",
    theme: "dark",
    language: "en-US",
  };

  const result = await kv.set(["preferences", "ada"], prefs);

  // type error
  // const result = await kv.set(["prefs", "ada"], prefs);
  // const result = await kv.set(["preferences", "ada"], "text");
}

{
  const entry = await kv.get(["preferences", "ada"]);
  console.log(entry.key); // ["preferences", "ada"]
  console.log(entry.value); // User | null
  console.log(entry.versionstamp);
}

{
  const result = await kv.getMany([
    ["preferences", "ada"],
    ["last_message_id"],
  ]);
  result[0].key; // ["preferences", UserId]
  result[0].value; // User | null
  result[0].versionstamp;
  result[1].key; // ["last_message_id"]
  result[1].value; // MessageId | null
  result[1].versionstamp;
}

{
  const entries = kv.list({ prefix: [] });
  for await (const entry of entries) {
    console.log(entry.key); // ["preferences", UserId] | ["messages", RoomId, MessageId] | ["last_message_id"]
    console.log(entry.value); // User | Message | MessageId
    console.log(entry.versionstamp);
  }
}

{
  const entries = kv.list({ prefix: ["preferences"] });
  for await (const entry of entries) {
    console.log(entry.key); // ["preferences", UserId]
    console.log(entry.value); // User
    console.log(entry.versionstamp);
  }
}

// Atomic transactions
{
  const key: ["preferences", UserId] = ["preferences", "alan"];
  const value = {
    username: "alan",
    theme: "light",
    language: "en-GB",
  };

  const res = await kv.atomic()
    .check({ key, versionstamp: null })
    .set(key, value)
    .commit();
  if (res.ok) {
    console.log("Preferences did not yet exist. Inserted!");
  } else {
    console.error("Preferences already exist.");
  }

  // type error
  //
  // const res = await kv.atomic()
  //   .check({ key: ["prefs", "alan"], versionstamp: null })
  //   ...
  //
  // const res = await kv.atomic()
  //   .set(["preferences", "alan"], "text")
  //   ...
}
```
