import { describe, expect, it } from "vitest";
import { GrainCallTimeoutError, RejectionError } from "@thresh/core/errors";
import { SiloAddress } from "@thresh/core/silo-address";
import {
  didNotExecute,
  mayResend,
  neverLeft,
  refusedBeforeExecution,
  thrownByTurn,
} from "@thresh/messaging/resend-policy";
import { connectFailure } from "@thresh/messaging/transport";

const peer = new SiloAddress("peer", "u", "peer:1");

describe("resend policy", () => {
  it("resends a stale-kind refusal the runtime made before the request ran", () => {
    expect(mayResend(refusedBeforeExecution(new RejectionError("moved", "noActivation")))).toBe(
      true,
    );
    expect(mayResend(refusedBeforeExecution(new RejectionError("stale", "staleView")))).toBe(true);
  });

  it("resends a request that never left: a failed dial or a send that threw", () => {
    expect(mayResend(connectFailure(peer, "refused", "refused"))).toBe(true);
    expect(mayResend(neverLeft(new RejectionError("closed", "unknownTarget")))).toBe(true);
  });

  it("never resends a stale-kind rejection that was not a refusal: a turn threw it", () => {
    const thrown = new RejectionError("a nested call found no activation", "noActivation");
    expect(didNotExecute(thrown)).toBe(false);
    expect(mayResend(thrown)).toBe(false);
  });

  it("never resends a nested call's refusal that a turn let escape: the turn's mark wins", () => {
    const nested = refusedBeforeExecution(new RejectionError("nested", "noActivation"));
    expect(mayResend(nested)).toBe(true);
    thrownByTurn(nested);
    expect(didNotExecute(nested)).toBe(false);
    expect(mayResend(nested)).toBe(false);
    // A refusal point the error passes afterwards cannot make it a refusal again.
    refusedBeforeExecution(nested);
    expect(mayResend(nested)).toBe(false);
    const dial = thrownByTurn(connectFailure(peer, "refused", "refused"));
    expect(didNotExecute(dial)).toBe(false);
  });

  it("never resends a reply timeout or a connection lost mid-call", () => {
    expect(mayResend(new GrainCallTimeoutError("timed out"))).toBe(false);
    expect(mayResend(new RejectionError("connection lost", "siloUnavailable"))).toBe(false);
  });

  it("does not resend a refusal whose kind re-resolving cannot change", () => {
    const overloaded = refusedBeforeExecution(new RejectionError("busy", "overloaded"));
    expect(didNotExecute(overloaded)).toBe(true);
    expect(mayResend(overloaded)).toBe(false);
  });
});
