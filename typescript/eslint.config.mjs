// @ts-check
import js from '@eslint/js';
import prettier from 'eslint-config-prettier';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  {
    ignores: ['dist/**', 'dist-examples/**', 'coverage/**', 'node_modules/**'],
  },

  js.configs.recommended,

  // Type-aware linting: catches un-awaited promises, unsafe `any` flow and the
  // mistakes a plain type check misses. Formatting is Prettier's job (see the
  // eslint-config-prettier block at the end, which switches those rules off).
  tseslint.configs.strictTypeChecked,
  tseslint.configs.stylisticTypeChecked,

  {
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
      globals: {
        ...globals.node,
      },
    },
    rules: {
      '@typescript-eslint/consistent-type-imports': ['error', { fixStyle: 'inline-type-imports' }],
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
      // The library models untrusted JSON from the wire, so `unknown` narrowing
      // stays; assertions would paper over it.
      '@typescript-eslint/no-unnecessary-condition': 'off',
      // Wire field names are read from untrusted JSON records; bracket access is
      // the right spelling for an index signature.
      '@typescript-eslint/dot-notation': ['error', { allowIndexSignaturePropertyAccess: true }],
      '@typescript-eslint/restrict-template-expressions': ['error', { allowNumber: true }],
    },
  },

  // Library sources must not assume a Node runtime: they use WebCrypto, fetch and
  // timers so the same code can run in a browser, Node or an edge worker.
  {
    files: ['src/**/*.ts'],
    rules: {
      'no-restricted-imports': [
        'error',
        {
          patterns: [
            {
              group: ['node:*', 'fs', 'path', 'crypto', 'http', 'events'],
              message:
                'Library sources must stay runtime-neutral; use Web APIs (globalThis.crypto, fetch, timers).',
            },
          ],
        },
      ],
    },
  },

  // Plain JavaScript (this config file itself) is linted but not type-checked.
  {
    files: ['**/*.{js,mjs,cjs}'],
    extends: [tseslint.configs.disableTypeChecked],
  },

  // Tests and the example are allowed to use Node and browser APIs freely.
  {
    files: ['test/**/*.ts', 'examples/**/*.ts', '*.config.ts'],
    languageOptions: {
      globals: {
        ...globals.node,
        ...globals.browser,
      },
    },
    rules: {
      'no-restricted-imports': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      // Async stubs are clearer than hand-rolled promise plumbing in tests.
      '@typescript-eslint/require-await': 'off',
    },
  },

  prettier,
);
