/**
 * Topic validation and MQTT-style matching.
 *
 * | Wildcard | Meaning                                                    |
 * |----------|------------------------------------------------------------|
 * | `+`      | exactly one segment                                        |
 * | `#`      | zero or more segments, only valid as the last segment      |
 *
 * Both wildcards must occupy a whole segment; a literal `+` or `#` inside a
 * segment must be escaped (`%2B`, `%23`) and is then matched as text.
 */

import { TopicError } from './errors.js';
import {
  HASH,
  PLUS,
  hasBareWildcardCharacter,
  isWildcardSegment,
  normalizeEscapes,
} from './escape.js';
import { splitTopic } from './topic.js';

export const DEFAULT_TOPIC_MAX_LENGTH = 512;

export interface TopicRules {
  /** Reject wildcards entirely. Servers that do not advertise wildcard support use this. */
  readonly allowWildcards?: boolean;
  readonly maxLength?: number;
}

// Rejecting control characters is the point of this regex.
// eslint-disable-next-line no-control-regex
const FORBIDDEN_CHARACTERS = /[\s\u0000-\u001f\u007f]/;

/**
 * Validates a topic or pattern and returns its normalised form.
 *
 * @throws {TopicError} with a stable `code`.
 */
export function validateTopic(value: unknown, rules: TopicRules = {}): string {
  const allowWildcards = rules.allowWildcards ?? true;
  const maxLength = rules.maxLength ?? DEFAULT_TOPIC_MAX_LENGTH;

  if (typeof value !== 'string') {
    throw new TopicError('A topic must be a string', 'invalid-type');
  }

  const topic = normalizeEscapes(value.trim());
  if (topic.length === 0) {
    throw new TopicError('A topic must not be empty', 'empty', topic);
  }
  if (topic.length > maxLength) {
    throw new TopicError(`A topic must not exceed ${maxLength} characters`, 'too-long', topic);
  }
  if (FORBIDDEN_CHARACTERS.test(topic)) {
    throw new TopicError(
      'A topic must not contain whitespace or control characters',
      'invalid-character',
      topic,
    );
  }

  const segments = splitTopic(topic);
  for (const [index, segment] of segments.entries()) {
    if (segment.length === 0) {
      throw new TopicError('A topic must not contain empty segments', 'empty-segment', topic);
    }
    if (isWildcardSegment(segment)) {
      if (!allowWildcards) {
        throw new TopicError(
          'Wildcards are not supported by this server',
          'wildcard-not-allowed',
          topic,
        );
      }
      if (segment === HASH && index !== segments.length - 1) {
        throw new TopicError(
          "The '#' wildcard must be the final segment",
          'wildcard-not-last',
          topic,
        );
      }
      continue;
    }
    if (hasBareWildcardCharacter(segment)) {
      throw new TopicError(
        `Wildcards must occupy a whole segment; escape literal characters in "${segment}" as %2B / %23`,
        'wildcard-must-fill-segment',
        topic,
      );
    }
  }

  return topic;
}

export interface TopicPattern {
  readonly value: string;
  readonly hasWildcards: boolean;
  matches(topic: string): boolean;
}

/** Exact topics short-circuit to a string comparison. */
export function compileTopicPattern(value: unknown, rules: TopicRules = {}): TopicPattern {
  const pattern = validateTopic(value, { ...rules, allowWildcards: true });
  const segments = splitTopic(pattern);
  const hasWildcards = segments.some(isWildcardSegment);

  return {
    value: pattern,
    hasWildcards,
    matches(topic: string): boolean {
      if (!hasWildcards) return pattern === normalizeEscapes(topic);
      return matchesSegments(segments, splitTopic(normalizeEscapes(topic)));
    },
  };
}

export function topicMatches(pattern: string, topic: string): boolean {
  return compileTopicPattern(pattern).matches(topic);
}

function matchesSegments(pattern: readonly string[], topic: readonly string[]) {
  let index = 0;
  for (; index < pattern.length; index += 1) {
    const segment = pattern[index];
    if (segment === HASH) return true; // '#' also matches zero remaining segments
    if (index >= topic.length) return false;
    if (segment !== PLUS && segment !== topic[index]) return false;
  }
  return index === topic.length;
}
