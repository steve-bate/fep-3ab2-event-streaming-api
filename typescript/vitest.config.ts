import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const entry = (name: string): string =>
  fileURLToPath(new URL(`./src/${name}/index.ts`, import.meta.url));

export default defineConfig({
  // Tests import the package by name, exactly as a consumer would; these aliases
  // point the subpaths at sources so a test run needs no prior build.
  resolve: {
    alias: {
      '@fep-3ab2/streaming/client': entry('client'),
      '@fep-3ab2/streaming/server': entry('server'),
      '@fep-3ab2/streaming/testing': entry('testing'),
      '@fep-3ab2/streaming/topics': entry('topics'),
      '@fep-3ab2/streaming': entry('topics'),
    },
  },
  test: {
    environment: 'node',
    include: ['test/**/*.test.ts'],
    restoreMocks: true,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      include: ['src/**/*.ts'],
      exclude: ['src/testing/**', 'src/**/index.ts'],
    },
  },
});
