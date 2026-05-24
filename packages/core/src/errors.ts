/** An error raised while dispatching or executing a grain call. */
export class GrainCallError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "GrainCallError";
  }
}

export type RejectionKind =
  | "siloDraining"
  | "unknownTarget"
  | "overloaded"
  | "deserialization"
  | "noActivation";

/** A runtime-level refusal the caller can inspect to decide whether to retry. */
export class RejectionError extends Error {
  constructor(
    message: string,
    readonly kind: RejectionKind,
  ) {
    super(message);
    this.name = "RejectionError";
  }
}

/** A grain call that did not receive a response within its deadline. */
export class GrainCallTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GrainCallTimeoutError";
  }
}
