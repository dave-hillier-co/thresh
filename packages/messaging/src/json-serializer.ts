import type { Serializer, SerializerOptions } from "@thresh/messaging/serializer";
import { decodeValue, encodeValue } from "@thresh/core/value-codec";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Human-readable serializer, useful for debugging and inspection. */
export class JsonSerializer implements Serializer {
  constructor(private readonly options: SerializerOptions = {}) {}

  serialize(value: unknown): Uint8Array {
    // JSON has no binary type, so a `Uint8Array` must travel as tagged base64 - passed through,
    // `JSON.stringify` would turn it into `{"0":1,...}` and the receiver would get a plain object.
    return encoder.encode(JSON.stringify(encodeValue(value, { binaryAsBase64: true })));
  }

  deserialize<T>(bytes: Uint8Array): T {
    return decodeValue(JSON.parse(decoder.decode(bytes)), this.options) as T;
  }
}
