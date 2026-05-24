import { decode, encode } from "@msgpack/msgpack";
import type { Serializer, SerializerOptions } from "@tsva/messaging/serializer";
import { decodeValue, encodeValue } from "@tsva/messaging/value-codec";

/** Compact binary serializer; the production default. */
export class MessagePackSerializer implements Serializer {
  constructor(private readonly options: SerializerOptions = {}) {}

  serialize(value: unknown): Uint8Array {
    return encode(encodeValue(value));
  }

  deserialize<T>(bytes: Uint8Array): T {
    return decodeValue(decode(bytes), this.options) as T;
  }
}
