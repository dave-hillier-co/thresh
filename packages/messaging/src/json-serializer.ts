import type { Serializer, SerializerOptions } from "@tsva/messaging/serializer";
import { decodeValue, encodeValue } from "@tsva/messaging/value-codec";

/** Human-readable serializer, useful for debugging and inspection. */
export class JsonSerializer implements Serializer {
  private readonly encoder = new TextEncoder();
  private readonly decoder = new TextDecoder();

  constructor(private readonly options: SerializerOptions = {}) {}

  serialize(value: unknown): Uint8Array {
    return this.encoder.encode(JSON.stringify(encodeValue(value)));
  }

  deserialize<T>(bytes: Uint8Array): T {
    return decodeValue(JSON.parse(this.decoder.decode(bytes)), this.options) as T;
  }
}
