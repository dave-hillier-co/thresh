import swc from "unplugin-swc";
import { defineConfig } from "vitest/config";

// esbuild (Vitest's default transformer) does not support standard TC39
// decorators, so transform with SWC's "2022-03" decorator proposal instead.
export default defineConfig({
  // Disable Vitest 4's default Oxc transform so SWC (below) owns transformation,
  // which is what gives us standard TC39 decorator support.
  oxc: false,
  plugins: [
    swc.vite({
      jsc: {
        parser: { syntax: "typescript", decorators: true },
        transform: { decoratorVersion: "2022-03" },
        target: "es2022",
        keepClassNames: true,
      },
    }),
  ],
  test: {
    include: ["packages/*/src/**/*.test.ts"],
    environment: "node",
  },
});
