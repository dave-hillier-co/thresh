// Ported from dotnet/orleans test/Grains/TestGrains/LogTestGrain.cs @ v10.1.0 (MIT).
// A real `JournaledGrain`: the `*Global` setters raise + confirm in one call
// (so `GetAGlobal`/`GetBothGlobal` observe them via the confirmed state), the
// `*Local` setters only raise (visible tentatively, until something confirms).
// Only the subset `LogTestGrainClearTests` exercises is ported — upstream's
// much larger surface (reservations, conditional writes, raw log reads) isn't
// needed here.
import { grain } from "@tsva/core/decorators";
import { JournaledGrain } from "@tsva/core/journaled-grain";
import {
  ILogTestGrain,
  type AB,
} from "@tsva/parity/grains/interfaces/log-test-grain-interfaces";

export { ILogTestGrain };

interface LogTestState {
  readonly a: number;
  readonly b: number;
}

type LogTestEvent =
  | { readonly kind: "updateA"; readonly val: number }
  | { readonly kind: "updateB"; readonly val: number }
  | { readonly kind: "incrementA" };

@grain({ name: "TestGrains.LogTestGrain" })
export class LogTestGrain extends JournaledGrain<LogTestState, LogTestEvent> implements ILogTestGrain {
  initialState(): LogTestState {
    return { a: 0, b: 0 };
  }

  transitionState(state: LogTestState, event: LogTestEvent): LogTestState {
    switch (event.kind) {
      case "updateA":
        return { ...state, a: event.val };
      case "updateB":
        return { ...state, b: event.val };
      case "incrementA":
        return { ...state, a: state.a + 1 };
    }
  }

  async setAGlobal(a: number): Promise<void> {
    this.raiseEvent({ kind: "updateA", val: a });
    await this.confirmEvents();
  }

  async setALocal(a: number): Promise<void> {
    this.raiseEvent({ kind: "updateA", val: a });
  }

  async incrementALocal(): Promise<void> {
    this.raiseEvent({ kind: "incrementA" });
  }

  async incrementAGlobal(): Promise<void> {
    this.raiseEvent({ kind: "incrementA" });
    await this.confirmEvents();
  }

  async setBGlobal(b: number): Promise<void> {
    this.raiseEvent({ kind: "updateB", val: b });
    await this.confirmEvents();
  }

  async setBLocal(b: number): Promise<void> {
    this.raiseEvent({ kind: "updateB", val: b });
  }

  async getAGlobal(): Promise<number> {
    await this.confirmEvents();
    return this.state.a;
  }

  async getALocal(): Promise<number> {
    return this.tentativeState.a;
  }

  async getBothGlobal(): Promise<AB> {
    await this.confirmEvents();
    return { a: this.state.a, b: this.state.b };
  }

  async getBothLocal(): Promise<AB> {
    return { a: this.tentativeState.a, b: this.tentativeState.b };
  }

  async getConfirmedVersion(): Promise<number> {
    return this.version;
  }

  async clear(): Promise<void> {
    await this.clearLog();
  }
}
