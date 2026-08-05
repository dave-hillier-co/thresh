import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { LogFields, Logger } from "@thresh/core/logger";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

interface Echoer extends GrainKey<string> {
  echo(s: string): Promise<string>;
  boom(): Promise<void>;
}
const Echoer = defineGrainInterface<Echoer>("LogEchoer");

@grain({ name: "LogEchoer" })
class EchoerGrain extends Grain implements Echoer {
  async echo(s: string): Promise<string> {
    return s;
  }
  async boom(): Promise<void> {
    throw new Error("kaboom");
  }
}

interface Record_ {
  level: string;
  message: string;
  fields?: LogFields;
}

function capturingLogger(records: Record_[]): Logger {
  const at =
    (level: string) =>
    (message: string, fields?: LogFields): void => {
      records.push({ level, message, ...(fields !== undefined ? { fields } : {}) });
    };
  return { debug: at("debug"), info: at("info"), warn: at("warn"), error: at("error") };
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(logger: Logger) {
  return createSilo({ clusterId: "logging", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useLogging(logger)
    .registerGrain(EchoerGrain, { interfaces: [Echoer] })
    .build();
}

describe("structured logging filter", () => {
  it("logs a successful call at info with structured fields", async () => {
    const records: Record_[] = [];
    const silo = buildSilo(capturingLogger(records));
    await silo.start();
    try {
      await silo.getGrain(Echoer, "e").echo("hi");
      const log = records.find((r) => r.message === "grain call");
      expect(log).toBeDefined();
      expect(log!.level).toBe("info");
      expect(log!.fields!.method).toBe("echo");
      expect(log!.fields!.interface).toBe("LogEchoer");
      expect(log!.fields!.status).toBe("ok");
      expect(typeof log!.fields!.durationMs).toBe("number");
    } finally {
      await silo.stop();
    }
  });

  it("logs a failing call at error with the message", async () => {
    const records: Record_[] = [];
    const silo = buildSilo(capturingLogger(records));
    await silo.start();
    try {
      await expect(silo.getGrain(Echoer, "e").boom()).rejects.toThrow("kaboom");
      const log = records.find((r) => r.message === "grain call failed");
      expect(log).toBeDefined();
      expect(log!.level).toBe("error");
      expect(log!.fields!.status).toBe("error");
      expect(log!.fields!.error).toBe("kaboom");
    } finally {
      await silo.stop();
    }
  });
});
