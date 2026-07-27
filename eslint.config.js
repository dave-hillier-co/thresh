import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["**/dist/**", "**/*.tsbuildinfo", "coverage/**", ".claude/**"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    rules: {
      // House style: no index.ts barrels; one primary export per file.
      "@typescript-eslint/consistent-type-imports": "error",
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
    },
  },
);
