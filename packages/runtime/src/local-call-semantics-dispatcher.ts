import type { InvocationRequest } from "@thresh/core/request";
import type { Dispatcher, InvokeCallOptions } from "@thresh/runtime/dispatcher";

/**
 * What a call through a grain reference does to its arguments, its result and a thrown error when
 * the target lives on the SAME silo (`SiloConfig.localCallSemantics`). A call to another silo
 * always serializes; this decides whether a local one behaves the same.
 *
 * - `"reference"`: nothing is copied. The callee receives the caller's own objects and the caller
 *   the callee's, a class instance keeps its class and an error its class. Cheapest; placement
 *   then changes what grain code can see and mutate.
 * - `"copy"`: the arguments and the result are deep-copied by `copyValue`, whose result is the
 *   codec round trip's without building the encoded form, so a callee sees exactly what it would
 *   from another silo; a value marked `immutable` is shared instead. A thrown error takes the
 *   codec's path, as under `"wire"`. Orleans' behaviour (its `DeepCopier` on local calls).
 * - `"wire"`: the arguments, the result and a thrown error make the full round trip through the
 *   silo's serializer, as a message body would. Exact by construction, and the most expensive;
 *   `immutable` has no effect.
 */
export type LocalCallSemantics = "reference" | "copy" | "wire";

/** How a value and an error come out the other side of a call. */
export interface LocalCallCopier {
  /** What the other side receives for `value`. */
  value(value: unknown): unknown;
  /** What a remote caller receives when a callee throws `error`. */
  error(error: unknown): unknown;
}

/**
 * Gives every call through a grain reference the semantics of a cross-silo call, wherever the
 * target lives: the arguments, the result and a thrown error each pass through `copier` (the
 * deep copier under `"copy"`, the serializer's round trip under `"wire"`). A same-silo call then
 * shares no object with its caller, a class instance with no surrogate arrives flattened (or is
 * refused) as on the wire, and an error arrives as the class the wire rebuilds.
 *
 * The wrapper sits in front of the whole dispatch, so a call that does go to another silo is
 * copied once here and serialized again on the wire; both are stable for any value the codec
 * accepts, so the second pass changes nothing the first did not.
 */
export class LocalCallSemanticsDispatcher implements Dispatcher {
  constructor(
    private readonly inner: Dispatcher,
    private readonly copier: LocalCallCopier,
  ) {}

  async invoke(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown> {
    const sent: InvocationRequest = { ...req, args: this.copier.value(req.args) as unknown[] };
    let result: unknown;
    try {
      result = await this.inner.invoke(sent, opts);
    } catch (error) {
      throw this.copier.error(error);
    }
    try {
      return this.copier.value(result);
    } catch (error) {
      // A result the codec refuses fails the callee's reply on the wire, and the caller gets
      // that failure.
      throw this.copier.error(error);
    }
  }
}
