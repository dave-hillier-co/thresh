import { describe, expect, it } from "vitest";
import type { Logger, LogFields } from "@thresh/core/logger";
import { GrainService } from "@thresh/runtime/grain-service";

function recordingLogger(): { logger: Logger; errors: Array<[string, LogFields | undefined]> } {
  const errors: Array<[string, LogFields | undefined]> = [];
  return {
    errors,
    logger: {
      debug() {},
      info() {},
      warn() {},
      error(message, fields) {
        errors.push([message, fields]);
      },
    },
  };
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("GrainService.start", () => {
  // Orleans does `StartInBackground().Ignore()` (GrainService.cs:95), which
  // swallows the rejection but still logs it — `void this.startInBackground()`
  // dropped it entirely, and under Node's default an un-caught rejection here
  // terminates the whole process rather than failing just this one service.
  it("logs a rejecting startInBackground() instead of producing an unhandled rejection", async () => {
    const { logger, errors } = recordingLogger();

    class FailingService extends GrainService {
      constructor() {
        super();
        this.logger = logger;
      }
      protected override async startInBackground(): Promise<void> {
        throw new Error("boom");
      }
    }

    const unhandled: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const service = new FailingService();
      service.start();
      await flush();

      expect(unhandled).toHaveLength(0);
      expect(
        errors.some(
          ([msg, fields]) => msg.includes("startInBackground") && fields?.error instanceof Error,
        ),
      ).toBe(true);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });
});
