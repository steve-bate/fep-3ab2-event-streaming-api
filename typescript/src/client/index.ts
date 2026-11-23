/**
 * Client entry point: session management, the streaming client, and discovery.
 */

export { StreamingClient, discoverStreamingEndpoint } from './stream.js';
export type { OpenOptions } from './stream.js';
export { StreamingClientError, toClientError } from './errors.js';
export type { StreamingClientErrorCode, StreamingClientErrorOptions } from './errors.js';
export {
  addSubscriptions,
  createStreamingSession,
  discoverStreamingControl,
  listSubscriptions,
  removeSubscription,
  revokeStreamingSession,
} from './session.js';
export type { SessionClientContext } from './session.js';
export { ProtocolError } from '../protocol.js';
export type * from './types.js';
