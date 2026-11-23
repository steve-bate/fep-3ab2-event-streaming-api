/**
 * Topic helpers shared by the client and server subpaths: URI → topic
 * derivation, MQTT-style wildcard matching, and validation.
 */

export {
  escapeWildcards,
  normalizeEscapes,
  isWildcardSegment,
  hasBareWildcardCharacter,
  PLUS,
  HASH,
} from './escape.js';
export { TopicError } from './errors.js';
export type { TopicErrorCode } from './errors.js';
export {
  DEFAULT_TOPIC_MAX_LENGTH,
  compileTopicPattern,
  topicMatches,
  validateTopic,
} from './pattern.js';
export type { TopicPattern, TopicRules } from './pattern.js';
export {
  TOPIC_SEPARATOR,
  joinTopic,
  splitTopic,
  topicFromUri,
  topicFromUriOrTopic,
} from './topic.js';
