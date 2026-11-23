import { describe, expect, it } from 'vitest';
import {
  TopicError,
  compileTopicPattern,
  escapeWildcards,
  joinTopic,
  splitTopic,
  topicFromUri,
  topicFromUriOrTopic,
  topicMatches,
  validateTopic,
} from '@fep-3ab2/streaming/topics';

describe('topicFromUri', () => {
  // The three examples from FEP-3ab2 §ActivityPub topics, verbatim.
  it.each([
    ['https://server.example/note/1', 'server.example/note/1'],
    ['https://server.example:1000/actor#xyz', 'server.example:1000/actor/xyz'],
    ['https://server.example:1000/actor+123#xyz', 'server.example:1000/actor%2B123/xyz'],
  ])('derives %s → %s', (uri, expected) => {
    expect(topicFromUri(uri)).toBe(expected);
  });

  it('drops the scheme and keeps the port', () => {
    expect(topicFromUri('http://example.org:8080/a/b')).toBe('example.org:8080/a/b');
  });

  it('collapses empty path segments and a root path', () => {
    expect(topicFromUri('https://example.org/')).toBe('example.org');
    expect(topicFromUri('https://example.org//a//b/')).toBe('example.org/a/b');
  });

  it('keeps a fragment inside one segment', () => {
    expect(topicFromUri('https://example.org/actor#a/b')).toBe('example.org/actor/a%2Fb');
  });

  it('escapes a hash inside the fragment', () => {
    expect(topicFromUri('https://example.org/note#frag%23ment')).toBe(
      'example.org/note/frag%23ment',
    );
  });

  it('accepts a URL instance', () => {
    expect(topicFromUri(new URL('https://example.org/note/9'))).toBe('example.org/note/9');
  });

  it('rejects non-HTTP schemes and relative references', () => {
    expect(() => topicFromUri('mailto:alice@example.org')).toThrow(TopicError);
    expect(() => topicFromUri('/note/1')).toThrow(TopicError);
  });
});

describe('topic helpers', () => {
  it('splits and joins without surprise', () => {
    expect(splitTopic('a/b/c')).toEqual(['a', 'b', 'c']);
    expect(joinTopic(['a', 'b'])).toBe('a/b');
  });

  it('escapes both wildcard characters', () => {
    expect(escapeWildcards('actor+123#x')).toBe('actor%2B123%23x');
  });

  it('accepts a URI anywhere a topic is expected', () => {
    expect(topicFromUriOrTopic('https://example.org/note/1')).toBe('example.org/note/1');
    expect(topicFromUriOrTopic('example.org/note/1')).toBe('example.org/note/1');
  });
});

describe('validateTopic', () => {
  it('normalises escape case', () => {
    expect(validateTopic('a/b%2bc')).toBe('a/b%2Bc');
  });

  it('accepts wildcards as whole segments', () => {
    expect(validateTopic('foo/+/baz')).toBe('foo/+/baz');
    expect(validateTopic('foo/#')).toBe('foo/#');
  });

  it('rejects empty, oversized and whitespace-bearing topics', () => {
    expect(() => validateTopic('')).toThrow(/empty/);
    expect(() => validateTopic('a b')).toThrow(TopicError);
    expect(() => validateTopic('x'.repeat(600))).toThrow(/exceed/);
    expect(() => validateTopic(42)).toThrow(/string/);
  });

  it('rejects empty segments', () => {
    expect(() => validateTopic('foo//bar')).toThrow(/empty segments/);
    expect(() => validateTopic('/foo')).toThrow(/empty segments/);
  });

  it("rejects a '#' that is not last", () => {
    expect(() => validateTopic('foo/#/bar')).toThrow(/final segment/);
  });

  it('rejects a wildcard character inside a segment', () => {
    expect(() => validateTopic('foo/a+b')).toThrow(/whole segment/);
    expect(() => validateTopic('foo/a#b')).toThrow(/whole segment/);
  });

  it('refuses wildcards when the server does not support them', () => {
    expect(() => validateTopic('foo/+', { allowWildcards: false })).toThrow(/not supported/);
    expect(() => validateTopic('foo/#', { allowWildcards: false })).toThrow(/not supported/);
    expect(validateTopic('foo/bar', { allowWildcards: false })).toBe('foo/bar');
  });

  it('treats an escaped wildcard as text', () => {
    expect(validateTopic('server.example/actor%2B123')).toBe('server.example/actor%2B123');
  });
});

describe('topic matching', () => {
  it('matches exact topics', () => {
    expect(topicMatches('server.example/note/1', 'server.example/note/1')).toBe(true);
    expect(topicMatches('server.example/note/1', 'server.example/note/2')).toBe(false);
    expect(topicMatches('server.example/note/1', 'server.example/note/1/extra')).toBe(false);
  });

  it("lets '+' match exactly one segment", () => {
    expect(topicMatches('foo/+/baz', 'foo/bar/baz')).toBe(true);
    expect(topicMatches('foo/+/baz', 'foo/baz')).toBe(false);
    expect(topicMatches('foo/+/baz', 'foo/bar/qux/baz')).toBe(false);
  });

  it("lets '#' match zero or more trailing segments", () => {
    expect(topicMatches('foo/#', 'foo')).toBe(true);
    expect(topicMatches('foo/#', 'foo/bar')).toBe(true);
    expect(topicMatches('foo/#', 'foo/bar/baz')).toBe(true);
    expect(topicMatches('foo/#', 'bar/foo')).toBe(false);
  });

  it('does not treat an escaped wildcard as a wildcard', () => {
    expect(topicMatches('server.example/actor%2B123', 'server.example/actor%2B123')).toBe(true);
    expect(topicMatches('server.example/actor%2B123', 'server.example/actor%2b123')).toBe(true);
    expect(topicMatches('server.example/actor%2B123', 'server.example/actorX123')).toBe(false);
  });

  it('exposes the compiled form', () => {
    const pattern = compileTopicPattern('server.example/#');
    expect(pattern.hasWildcards).toBe(true);
    expect(pattern.value).toBe('server.example/#');
    expect(pattern.matches('server.example/note/1')).toBe(true);
  });

  it('skips wildcard compilation for exact topics', () => {
    expect(compileTopicPattern('a/b').hasWildcards).toBe(false);
  });
});
