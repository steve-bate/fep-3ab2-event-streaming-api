/**
 * Topic derivation from ActivityPub URIs.
 *
 * 1. the scheme is removed;
 * 2. the authority (host, plus port when present) is the first segment;
 * 3. the URL path supplies the remaining `/`-separated segments;
 * 4. a URL fragment, if present, becomes the final segment;
 * 5. the wildcard characters `+` and `#` are percent-escaped.
 */

import { TopicError } from './errors.js';
import { escapeWildcards, normalizeEscapes } from './escape.js';

export const TOPIC_SEPARATOR = '/';

/** No validation, no unescaping */
export function splitTopic(topic: string): string[] {
  return topic.split(TOPIC_SEPARATOR);
}

export function joinTopic(segments: readonly string[]): string {
  return segments.join(TOPIC_SEPARATOR);
}

/**
 * Derives the topic for an ActivityPub entity URI.
 *
 * @example
 * topicFromUri('https://server.example/note/1');                       // 'server.example/note/1'
 * topicFromUri('https://server.example:1000/actor#xyz');               // 'server.example:1000/actor/xyz'
 * topicFromUri('https://server.example:1000/actor+123#xyz');           // 'server.example:1000/actor%2B123/xyz'
 */
export function topicFromUri(input: string | URL): string {
  const url = toUrl(input);
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new TopicError(
      `Cannot derive a topic from a non-HTTP URI: ${url.href}`,
      'invalid-character',
      url.href,
    );
  }

  const segments: string[] = [escapeWildcards(normalizeEscapes(url.host))];

  for (const segment of splitTopic(url.pathname)) {
    // Leading, trailing and repeated slashes carry no addressable segment.
    if (segment.length === 0) continue;
    segments.push(escapeWildcards(normalizeEscapes(segment)));
  }

  const fragment = url.hash.slice(1);
  if (fragment.length > 0) {
    // A fragment must stay a single segment, so a slash inside it is escaped.
    segments.push(escapeWildcards(normalizeEscapes(fragment)).replaceAll(TOPIC_SEPARATOR, '%2F'));
  }

  return joinTopic(segments);
}

/** Convenience for UI code where a user may paste either form. */
export function topicFromUriOrTopic(value: string): string {
  const trimmed = value.trim();
  if (/^https?:\/\//i.test(trimmed)) return topicFromUri(trimmed);
  return normalizeEscapes(trimmed);
}

function toUrl(input: string | URL) {
  if (input instanceof URL) return input;
  if (typeof input !== 'string') {
    throw new TopicError('topicFromUri() requires a string or URL', 'invalid-type');
  }
  const candidate = input.trim();
  if (candidate.length === 0) {
    throw new TopicError('topicFromUri() received an empty URI', 'empty');
  }
  try {
    return new URL(candidate);
  } catch {
    throw new TopicError(`Not an absolute URI: ${candidate}`, 'invalid-character', candidate);
  }
}
