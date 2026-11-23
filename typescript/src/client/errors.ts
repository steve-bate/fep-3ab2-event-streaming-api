import type { ProblemDetails } from '../protocol.js';

export type StreamingClientErrorCode =
  'http' | 'protocol' | 'not-connected' | 'closed' | 'transport';

export interface StreamingClientErrorOptions {
  readonly code: StreamingClientErrorCode;
  readonly status?: number;
  readonly body?: unknown;
  /** RFC 9457 problem details, when the server answered with one */
  readonly problem?: ProblemDetails;
  readonly cause?: unknown;
}

export class StreamingClientError extends Error {
  override readonly name = 'StreamingClientError';
  readonly code: StreamingClientErrorCode;
  /** HTTP status, when the failure came from the control API. */
  readonly status: number | undefined;
  /** Parsed response body, when the server sent one */
  readonly body: unknown;
  /**
   * The RFC 9457 problem details the server sent; branch on
   * `problem.type` rather than on the message.
   */
  readonly problem: ProblemDetails | undefined;

  constructor(message: string, options: StreamingClientErrorOptions) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.code = options.code;
    this.status = options.status;
    this.body = options.body;
    this.problem = options.problem;
  }
}

export function toClientError(error: unknown): StreamingClientError {
  if (error instanceof StreamingClientError) return error;
  const message = error instanceof Error ? error.message : String(error);
  return new StreamingClientError(message, { code: 'transport', cause: error });
}
