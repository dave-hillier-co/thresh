import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const files = [
  "docs/guide/index.md",
  "docs/guide/quick-start.md",
  "docs/guide/grains.md",
  "docs/guide/state.md",
  "docs/guide/background-work.md",
  "docs/guide/streams.md",
  "docs/guide/hosting.md",
  "docs/guide/testing.md",
  "docs/reference/packages.md",
  "docs/reference/agents.md",
  "docs/deviations.md",
  "docs/typescript-grain-api.md",
];
const sections = await Promise.all(
  files.map(
    async (file) =>
      `\n\n---\nSource: ${file}\n---\n\n${await readFile(resolve(root, file), "utf8")}`,
  ),
);
await mkdir(resolve(root, "docs"), { recursive: true });
await writeFile(
  resolve(root, "docs/llms-full.txt"),
  `# Thresh full documentation${sections.join("")}\n`,
);
