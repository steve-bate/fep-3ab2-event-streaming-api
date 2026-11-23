/**
 * Escaping of the two MQTT wildcard characters.
 *
 * A topic is `/`-delimited text, and `+` / `#` are wildcards.
 * A *literal* plus or hash inside a segment is therefore written percent-escaped
 * (`%2B`, `%23`); publishers must escape them, subscribers must escape any that
 * are not wildcards. Escapes are normalised to upper case so that `%2b` and `%2B`
 * denote the same topic and compare equal.
 */

export const PLUS = '+';
export const HASH = '#';

const WILDCARD_ESCAPES: Readonly<Record<string, string>> = {
  [PLUS]: '%2B',
  [HASH]: '%23',
};

export function escapeWildcards(value: string): string {
  return value.replace(/[+#]/g, (character) => WILDCARD_ESCAPES[character] ?? character);
}

/** Upper-cases the escapes so equivalent topics compare equal */
export function normalizeEscapes(value: string): string {
  return value.replace(/%(?:2b|23)/gi, (escape) => escape.toUpperCase());
}

export function hasBareWildcardCharacter(segment: string): boolean {
  return segment.includes(PLUS) || segment.includes(HASH);
}

export function isWildcardSegment(segment: string): boolean {
  return segment === PLUS || segment === HASH;
}
