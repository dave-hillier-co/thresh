import type { InvocationRequest } from "@thresh/core/request";
import type { Dispatcher, InvokeCallOptions } from "@thresh/runtime/dispatcher";

/** How a value and an error come out the other side of the wire. */
export interface WireRoundTrip {
  /** Serialize and deserialize `value` exactly as a message body is. */
  value(value: unknown): unknown;
  /** What a remote caller receives when a callee throws `error`. */
  error(error: unknown): unknown;
}

/**
 * Gives every call through a grain reference the semantics of a cross-silo call, wherever the
 * target lives (`SiloConfig.wireSemanticsForLocalCalls`): the arguments, the result and a thrown
 * error each make the codec round trip a message would. A same-silo call then shares no object
 * with its caller, a class instance with no surrogate arrives flattened (or is refused) as on the
 * wire, and an error arrives as the class the wire rebuilds. Orleans gets the first of these by
 * deep-copying arguments and responses even for local calls; here the codec is the copier, so the
 * other two follow from it.
 *
 * The wrapper sits in front of the whole dispatch, so a call that does go to another silo makes
 * the round trip once here and again on the wire; the codec's round trip is stable for any value
 * it accepts, so the second pass changes nothing the first did not.
 */
export class WireSemanticsDispatcher implements Dispatcher {
  constructor(
    private readonly inner: Dispatcher,
    private readonly wire: WireRoundTrip,
  ) {}

  async invoke(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown> {
    const sent: InvocationRequest = { ...req, args: this.wire.value(req.args) as unknown[] };
    let result: unknown;
    try {
      result = await this.inner.invoke(sent, opts);
    } catch (error) {
      throw this.wire.error(error);
    }
    try {
      return this.wire.value(result);
    } catch (error) {
      // A result the codec refuses fails the callee's reply on the wire, and the caller gets
      // that failure.
      throw this.wire.error(error);
    }
  }
}
