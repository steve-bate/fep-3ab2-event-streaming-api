/**
 * Topic strings arrive from clients (subscription requests) and from application
 * code (publish calls), so every failure carries a stable machine code the HTTP
 * layer can turn into a `400` body.
 */

export type TopicErrorCode =
  | 'invalid-type'
  | 'empty'
  | 'too-long'
  | 'invalid-character'
  | 'empty-segment'
  | 'wildcard-not-allowed'
  | 'wildcard-must-fill-segment'
  | 'wildcard-not-last';

export class TopicError extends Error {
  override readonly name = 'TopicError';
  readonly code: TopicErrorCode;
  readonly topic: string | undefined;

  constructor(message: string, code: TopicErrorCode, topic?: string) {
    super(message);
    this.code = code;
    this.topic = topic;
  }
}
