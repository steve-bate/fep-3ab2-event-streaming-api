/**
 * The page's origin: serves the demo page and the built library as ESM.
 *
 * By default the page is on `:5180` and the API on `:5181`, so every call the page
 * makes is cross-origin and exercises the FEP's strict CORS path. Pass `apiApp` to
 * mount the API here instead (single-origin mode), which is what you want when only
 * one port is reachable, for example when the demo is port-forwarded from another
 * machine and there is no forward for the API port.
 */

import { readFile } from 'node:fs/promises';
import path from 'node:path';
import express, { type Express } from 'express';
import { transform } from 'esbuild';

export interface WebServerOptions {
  /** Port the FEP-3ab2 API listens on; the page calls it on its own host name. */
  readonly apiPort: number;
  /**
   * Absolute origin the page should call, such as `https://api.example`. Omit it to
   * use the host the page was loaded from plus `apiPort`, which keeps
   * `localhost` and `127.0.0.1` working as separate origins.
   */
  readonly apiOrigin?: string;
  /**
   * Mount this API on the page's own origin. When present the page uses relative
   * URLs and no second port is needed (CORS is then only exercised by the
   * preflight/same-origin path, which is the trade-off).
   */
  readonly apiApp?: Express;
}

export interface WebServer {
  readonly app: Express;
}

const here = import.meta.dirname;
const projectRoot = path.resolve(here, '../..');
const publicDir = path.join(here, 'public');
const appEntry = path.join(here, 'app.ts');
const libraryDir = path.join(projectRoot, 'dist');

export function createWebServer(options: WebServerOptions): WebServer {
  const app = express();
  app.disable('x-powered-by');

  const singleOrigin = options.apiApp !== undefined;

  // The page carries its API origin in a meta tag; inject the configured one so
  // the demo can be run on non-default ports without editing HTML.
  app.get(['/', '/index.html'], async (req, res) => {
    try {
      // Point the page at the API on the same host name it was loaded from, so
      // `localhost` and `127.0.0.1` both work (they are distinct origins and
      // distinct cookie hosts). An empty value means "this origin".
      const host = (req.headers.host ?? '').split(':')[0] ?? 'localhost';
      const pageHost = host.length > 0 ? host : 'localhost';
      const apiOrigin = singleOrigin
        ? ''
        : (options.apiOrigin ?? `${req.protocol}://${pageHost}:${options.apiPort}`);
      const html = await readFile(path.join(publicDir, 'index.html'), 'utf8');
      res.type('text/html').send(html.replaceAll('__API_ORIGIN__', apiOrigin));
    } catch (error) {
      res
        .status(500)
        .type('text/plain')
        .send(`Failed to read index.html: ${String(error)}`);
    }
  });

  // The browser app is TypeScript; esbuild transpiles it on request so the demo
  // needs no bundler step of its own.
  app.get('/app.js', async (_req, res) => {
    try {
      const source = await readFile(appEntry, 'utf8');
      const result = await transform(source, {
        loader: 'ts',
        format: 'esm',
        target: 'es2022',
        sourcemap: 'inline',
        sourcefile: 'app.ts',
      });
      res.type('text/javascript').send(result.code);
    } catch (error) {
      res
        .status(500)
        .type('text/plain')
        .send(`Failed to build app.ts: ${String(error)}`);
    }
  });

  // Single-origin mode: the API's own routes, actor document included.
  if (options.apiApp !== undefined) app.use(options.apiApp);

  // The import map in index.html points `@fep-3ab2/streaming/*` at these files.
  app.use('/lib', express.static(libraryDir, { fallthrough: false }));

  app.use(express.static(publicDir, { index: false }));

  return { app };
}
