import { decode, encode } from "@msgpack/msgpack";
import type { Serializer, SerializerOptions } from "@tsva/messaging/serializer";
import { decodeValue, encodeValue } from "@tsva/core/value-codec";

/** Compact binary serializer; the production default. */
export class MessagePackSerializer implements Serializer {
  constructor(private readonly options: SerializerOptions = {}) {}

  serialize(value: unknown): Uint8Array {
    // ignoreUndefined so optional envelope fields round-trip as absent, not null.
    return encode(encodeValue(value), { ignoreUndefined: true });
  }

  deserialize<T>(bytes: Uint8Array): T {
    return decodeValue(decode(bytes), this.options) as T;
  }
}
