import type { Serializer, SerializerOptions } from "@thresh/messaging/serializer";
import { decodeValue, encodeValue } from "@thresh/core/value-codec";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Human-readable serializer, useful for debugging and inspection. */
export class JsonSerializer implements Serializer {
  constructor(private readonly options: SerializerOptions = {}) {}

  serialize(value: unknown): Uint8Array {
    return encoder.encode(JSON.stringify(encodeValue(value)));
  }

  deserialize<T>(bytes: Uint8Array): T {
    return decodeValue(JSON.parse(decoder.decode(bytes)), this.options) as T;
  }
}
