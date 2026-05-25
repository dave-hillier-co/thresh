import type { QueueEntry, RedisStreamQueue } from "@tsva/streams/redis-stream-queue";

/** Delivers one pulled event to the stream's subscribers; the agent supplies it. */
export type DeliverEvent = (streamKey: string, event: unknown, token: number) => Promise<void>;

export interface QueuePullingAgentOptions {
  /** How often to poll the queue when idle (defaults to 50ms). */
  pollIntervalMs?: number;
  /** Maximum entries read per poll (defaults to 128). */
  batchSize?: number;
}

/**
 * Pulls one physical queue and delivers each entry to its subscribers, then
 * commits the queue cursor — so delivery is at-least-once and a successor agent
 * (after a membership change) resumes from the committed position with no gaps.
 * Mirrors an Orleans persistent-stream pulling agent. One agent runs per queue a
 * silo owns; `PullingAgentManager` (a later slice) starts/stops them as ring
 * ownership changes.
 */
export class QueuePullingAgent {
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private cursor: number | undefined;
  private pumping = false;
  private running = false;

  constructor(
    private readonly queue: RedisStreamQueue,
    private readonly deliver: DeliverEvent,
    options: QueuePullingAgentOptions = {},
  ) {
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
    this.batchSize = options.batchSize ?? 128;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.schedule(0);
  }

  stop(): void {
    this.running = false;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
  }

  private schedule(delayMs: number): void {
    if (!this.running) return;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.pump();
    }, delayMs);
  }

  private async pump(): Promise<void> {
    if (this.pumping || !this.running) return;
    this.pumping = true;
    try {
      // Resume from the durably committed cursor on first pump (covers a
      // successor that took the queue over from another silo).
      if (this.cursor === undefined) this.cursor = await this.queue.getCursor();
      const entries: QueueEntry[] = await this.queue.readAfter(this.cursor, this.batchSize);
      for (const { token, streamKey, event } of entries) {
        if (!this.running) break;
        try {
          await this.deliver(streamKey, event, token);
        } catch {
          break; // leave the cursor; this entry is redelivered on the next poll
        }
        this.cursor = token;
        await this.queue.commit(token); // commit only after delivery (at-least-once)
      }
    } finally {
      this.pumping = false;
      if (this.running) this.schedule(this.pollIntervalMs);
    }
  }
}
