/**
 * The demo page's client code.
 *
 * It uses the library exactly as an application would: discover
 * `endpoints.streamingControl` from the actor document, construct a
 * `StreamingClient`, subscribe to topics, and render the activities that arrive
 * on the single multiplexed stream.
 */

import {
  StreamingClient,
  discoverStreamingEndpoint,
  type StreamingActivity,
  type StreamingClientState,
} from '@fep-3ab2/streaming/client';
import { topicFromUri } from '@fep-3ab2/streaming/topics';

const metaApiOrigin = document.querySelector<HTMLMetaElement>('meta[name="api-origin"]')?.content;
const apiOrigin = (
  new URLSearchParams(location.search).get('api') ??
  metaApiOrigin ??
  `${location.protocol}//${location.hostname}:5181`
).replace(/\/+$/, '');

function element(id: string): HTMLElement {
  const found = document.getElementById(id);
  if (found === null) throw new Error(`Missing #${id}`);
  return found;
}

const input = (id: string) => element(id) as HTMLInputElement;
const button = (id: string) => element(id) as HTMLButtonElement;
const list = (id: string) => element(id) as HTMLUListElement;

const ui = {
  pageOrigin: element('page-origin'),
  apiOrigin: element('api-origin'),
  username: input('username'),
  password: input('password'),
  login: button('login'),
  authBadge: element('auth-badge'),
  connect: button('connect'),
  disconnect: button('disconnect'),
  stateBadge: element('state-badge'),
  events: element('events'),
  clearEvents: button('clear-events'),
  log: element('log'),
  topic: input('topic'),
  addTopic: button('add-topic'),
  topics: list('topics'),
  suggestInbox: button('suggest-1'),
  suggestActor: button('suggest-2'),
  noteContent: input('note-content'),
  createNote: button('create-note'),
  useAdd: input('use-add'),
  timeline: element('timeline'),
  statEvents: element('stat-events'),
  statReconnects: element('stat-reconnects'),
  statExpiry: element('stat-expiry'),
  sessionUrls: element('session-urls'),
};

ui.pageOrigin.textContent = location.origin;
ui.apiOrigin.textContent = apiOrigin;

let actorUri: string | undefined;
let client: StreamingClient | undefined;

/**
 * The actor the page talks to. Before sign-in this is inferred from the API
 * origin, so the topic examples fill in either way, since the actor document is public.
 */
const demoActor = () => actorUri ?? `${apiOrigin === '' ? location.origin : apiOrigin}/users/alice`;

const inboxTopicOf = (actor: string) => topicFromUri(`${actor}/inbox`);
let eventCount = 0;
let reconnectCount = 0;

// helpers

function log(message: string) {
  const stamp = new Date().toLocaleTimeString();
  ui.log.textContent = `${stamp}  ${message}\n${ui.log.textContent ?? ''}`.trim();
}

function setBadge(badge: HTMLElement, text: string, state: string) {
  badge.textContent = text;
  badge.className = `badge ${state}`;
}

const apiLabel = apiOrigin === '' ? '(same origin)' : apiOrigin;

async function request(path: string, init: RequestInit = {}) {
  try {
    return await fetch(`${apiOrigin}${path}`, { credentials: 'include', ...init });
  } catch {
    // A blocked CORS request and a missing server are indistinguishable to `fetch`.
    throw new Error(
      `cannot reach ${apiOrigin === '' ? location.origin : apiOrigin}${path} — the API is not answering there. ` +
        `If you forwarded only this page's port, forward the API port too, or start the demo with DEMO_SINGLE_ORIGIN=1 ` +
        `(one port, API mounted on the page's origin). CORS is only involved once the request actually arrives: ` +
        `this page's origin is ${location.origin}.`,
    );
  }
}

async function requireOk(response: Response) {
  if (response.ok) return response;
  // Errors are RFC 9457 problem details.
  const payload = (await response.json().catch(() => undefined)) as
    { title?: string; detail?: string } | undefined;
  const reason = payload?.detail ?? payload?.title ?? 'request failed';
  // Keep the HTTP status in the message so a wrong password (401) reads differently
  // from a bad request (400) in the log.
  throw new Error(`${reason} (HTTP ${response.status})`);
}

function renderTopics(topics: readonly string[]) {
  ui.topics.replaceChildren(
    ...topics.map((topic) => {
      const item = document.createElement('li');
      item.className = 'topic';

      const label = document.createElement('span');
      label.textContent = topic;

      const remove = document.createElement('button');
      remove.textContent = 'remove';
      remove.className = 'danger';
      remove.addEventListener('click', () => {
        void removeTopic(topic);
      });

      item.append(label, remove);
      return item;
    }),
  );

  if (topics.length === 0) {
    const empty = document.createElement('li');
    empty.className = 'hint';
    empty.textContent = 'No topics subscribed. Add one to receive events.';
    ui.topics.append(empty);
  }
}

function renderEvent(activity: StreamingActivity) {
  const entry = document.createElement('div');
  entry.className = 'event';

  const meta = document.createElement('div');
  meta.className = 'meta';
  meta.textContent = `${new Date(activity.receivedAt).toLocaleTimeString()} · ${activity.event} · id ${activity.id}`;

  // The wire payload, exactly as it arrived.
  const body = document.createElement('pre');
  body.className = 'body';
  body.textContent = JSON.stringify(activity.data, null, 2);

  entry.append(meta, body);
  ui.events.prepend(entry);

  eventCount += 1;
  ui.statEvents.textContent = String(eventCount);
  ui.clearEvents.disabled = false;
}

function updateCreateButton() {
  ui.createNote.disabled = actorUri === undefined || ui.noteContent.value.trim().length === 0;
}

function renderState(state: StreamingClientState) {
  setBadge(ui.stateBadge, state, state);

  const signedIn = actorUri !== undefined;
  const connected = state === 'open' || state === 'connecting' || state === 'reconnecting';

  // Nothing but sign-in is usable before there is an actor: the topic examples
  // and the note field would otherwise accept input that could never be sent.
  ui.connect.disabled = !signedIn || connected;
  ui.disconnect.disabled = !connected;
  ui.topic.disabled = !signedIn;
  ui.addTopic.disabled = !connected;
  ui.suggestInbox.disabled = !signedIn;
  ui.suggestActor.disabled = !signedIn;
  ui.noteContent.disabled = !signedIn;
  ui.useAdd.disabled = !signedIn;
  updateCreateButton();
}

// actions

async function signIn() {
  ui.login.disabled = true;
  try {
    const credentials = btoa(`${ui.username.value}:${ui.password.value}`);
    const authorization = ['Basic', credentials].join(' ');
    const response = await requireOk(
      await request('/demo/login', {
        method: 'POST',
        headers: { authorization },
      }),
    );
    const payload = (await response.json()) as { actor: string };
    actorUri = payload.actor;
    ui.topic.value = inboxTopicOf(actorUri);

    setBadge(ui.authBadge, 'signed in', 'open');
    renderState('idle');
    log(`signed in as ${actorUri}`);
  } catch (error) {
    setBadge(ui.authBadge, 'sign-in failed', 'closed');
    const detail = error instanceof Error ? error.message : String(error);
    const hint = detail.includes('HTTP 401')
      ? ` — sent user "${ui.username.value}"; the password field must read "demo" (browser autofill can replace it); the API allowlists ${location.origin}`
      : '';
    log(`sign-in failed: ${detail}${hint}`);
  } finally {
    ui.login.disabled = false;
  }
}

async function connect() {
  if (actorUri === undefined || client !== undefined) return;
  ui.connect.disabled = true;

  try {
    // Discovery: the actor document names the control endpoint.
    const streamingControl = await discoverStreamingEndpoint(actorUri);
    log(`discovered control endpoint ${streamingControl}`);

    const streamingClient = new StreamingClient({
      streamingControl,
      topics: [inboxTopicOf(demoActor())],
    });
    client = streamingClient;

    streamingClient.on('state', (state) => {
      renderState(state);
      if (state === 'reconnecting') reconnectCount += 1;
      ui.statReconnects.textContent = String(reconnectCount);
      log(`state → ${state}`);
    });

    streamingClient.on('session', (session) => {
      ui.statExpiry.textContent = new Date(session.expiresAt).toLocaleTimeString();
      const heartbeat =
        session.heartbeatIntervalSeconds === undefined
          ? 'none advertised'
          : `${session.heartbeatIntervalSeconds}s`;
      ui.sessionUrls.textContent = `stream ${session.streamUrl} · subscriptions ${session.subscriptionsUrl} · wildcards ${session.wildcardSupport ? 'on' : 'off'} · heartbeat ${heartbeat}`;
    });

    streamingClient.on('subscriptions', (topics) => {
      renderTopics(topics);
    });

    streamingClient.on('activity', (activity) => {
      renderEvent(activity);
      applyActivity(activity);
    });

    streamingClient.on('heartbeat', () => {
      log('heartbeat');
    });

    streamingClient.on('error', (error) => {
      log(`error: ${error.message}`);
    });

    await streamingClient.open();
    // The server is the source of truth for subscriptions; render what it returns
    // (the mirror may already have matched, in which case no event fired).
    renderTopics(await streamingClient.refreshSubscriptions());
    log('stream opened');
  } catch (error) {
    log(`connect failed: ${error instanceof Error ? error.message : String(error)}`);
    client = undefined;
    renderState('closed');
  }
}

async function disconnect() {
  const current = client;
  client = undefined;
  if (current !== undefined) {
    await current.close();
    log('stream closed and session revoked');
  }
  renderState('closed');
}

async function addTopic(topic: string) {
  if (client === undefined || topic.trim().length === 0) return;
  try {
    const topics = await client.subscribe(topic.trim());
    renderTopics(topics);
    log(`subscribed to ${topic.trim()}`);
  } catch (error) {
    log(`subscribe failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

async function removeTopic(topic: string) {
  if (client === undefined) return;
  try {
    const topics = await client.unsubscribe(topic);
    renderTopics(topics);
    log(`unsubscribed from ${topic}`);
  } catch (error) {
    log(`unsubscribe failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/** A note as the timeline sees it, rebuilt from the stream. */
interface TimelineNote {
  readonly id: string;
  readonly content: string;
  readonly updated: boolean;
}

const notes = new Map<string, TimelineNote>();

function activity(type: string, object: Record<string, unknown>) {
  if (actorUri === undefined) throw new Error('sign in first');
  return {
    '@context': 'https://www.w3.org/ns/activitystreams',
    id: `${new URL(actorUri).origin}/activities/${crypto.randomUUID()}`,
    type,
    actor: actorUri,
    object,
  };
}

/** POSTs an activity to the actor's inbox; inbox processing publishes it to the topic. */
async function postToInbox(payload: Record<string, unknown>, description: string) {
  if (actorUri === undefined) return;
  const username = new URL(actorUri).pathname.split('/').pop() ?? 'alice';

  try {
    const response = await requireOk(
      await request(`/users/${username}/inbox`, {
        method: 'POST',
        headers: { 'content-type': 'application/activity+json' },
        body: JSON.stringify(payload),
      }),
    );
    const result = (await response.json()) as { topic: string; delivered: number };
    log(`${description} → ${result.topic}, ${result.delivered} stream(s)`);
  } catch (error) {
    log(`inbox post failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/** `Add{Create{Note}}`: the Create as an object of an Add, targeting the inbox. */
function wrapInAdd(inner: Record<string, unknown>) {
  if (actorUri === undefined) throw new Error('sign in first');
  return {
    '@context': 'https://www.w3.org/ns/activitystreams',
    id: `${new URL(actorUri).origin}/activities/${crypto.randomUUID()}`,
    type: 'Add',
    actor: actorUri,
    target: `${actorUri}/inbox`,
    object: inner,
  };
}

function createNoteFromInput() {
  if (actorUri === undefined) return;
  const content = ui.noteContent.value.trim();
  if (content.length === 0) {
    log('type some content first');
    return;
  }

  const noteId = `${new URL(actorUri).origin}/notes/${crypto.randomUUID()}`;
  const create = activity('Create', { id: noteId, type: 'Note', attributedTo: actorUri, content });
  const useAdd = ui.useAdd.checked;

  ui.noteContent.value = '';
  updateCreateButton();
  void postToInbox(
    useAdd ? wrapInAdd(create) : create,
    `${useAdd ? 'Add{Create{Note}}' : 'Create{Note}'} ${noteId}`,
  );
}

function updateNote(note: TimelineNote, content: string) {
  void postToInbox(
    activity('Update', {
      id: note.id,
      type: 'Note',
      attributedTo: actorUri,
      content,
      updated: new Date().toISOString(),
    }),
    `Update{Note} ${note.id}`,
  );
}

function deleteNote(note: TimelineNote) {
  void postToInbox(activity('Delete', { id: note.id, type: 'Tombstone' }), `Delete ${note.id}`);
}

/**
 * Folds a received activity into the timeline. Create adds a note, Update replaces
 * its content, Delete removes it; the client keeps no state of its own.
 */
function applyActivity(activity: StreamingActivity) {
  let payload = activity.data;
  if (typeof payload !== 'object' || payload === null) return;

  // `Add{Create{Note}}` wraps the same note; fold the inner Create instead.
  if ((payload as { type?: unknown }).type === 'Add') {
    const inner = (payload as { object?: unknown }).object;
    if (typeof inner === 'object' && inner !== null) payload = inner;
  }

  const type = (payload as { type?: unknown }).type;
  const object = (payload as { object?: unknown }).object;
  const noteId = valueId(object);
  if (noteId === undefined) return;

  if (type === 'Create' || type === 'Update') {
    const existing = notes.get(noteId);
    notes.set(noteId, {
      id: noteId,
      content: valueContent(object) ?? existing?.content ?? '',
      updated: type === 'Update',
    });
  } else if (type === 'Delete') {
    notes.delete(noteId);
  } else {
    return;
  }

  renderTimeline();
}

function valueId(object: unknown) {
  if (typeof object === 'string') return object;
  if (typeof object !== 'object' || object === null) return undefined;
  const id = (object as { id?: unknown }).id;
  return typeof id === 'string' ? id : undefined;
}

function valueContent(object: unknown) {
  if (typeof object !== 'object' || object === null) return undefined;
  const content = (object as { content?: unknown }).content;
  return typeof content === 'string' ? content : undefined;
}

function renderTimeline() {
  const items = [...notes.values()].reverse();

  if (items.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'hint';
    empty.textContent = 'No notes yet. Create one above; the timeline is built from stream events.';
    ui.timeline.replaceChildren(empty);
    return;
  }

  ui.timeline.replaceChildren(...items.map(noteCard));
}

function noteCard(note: TimelineNote): HTMLElement {
  const card = document.createElement('div');
  card.className = 'note';

  const meta = document.createElement('div');
  meta.className = 'note-meta';
  meta.textContent = `${note.updated ? 'updated · ' : ''}${note.id}`;

  const content = document.createElement('input');
  content.value = note.content;
  content.setAttribute('aria-label', 'Note content');

  const update = document.createElement('button');
  update.textContent = 'Update';
  update.addEventListener('click', () => {
    updateNote(note, content.value);
  });

  const remove = document.createElement('button');
  remove.className = 'danger';
  remove.textContent = 'Delete';
  remove.addEventListener('click', () => {
    deleteNote(note);
  });

  const row = document.createElement('div');
  row.className = 'row';
  row.append(content, update, remove);

  card.append(meta, row);
  return card;
}

// wiring

ui.login.addEventListener('click', () => {
  void signIn();
});
ui.connect.addEventListener('click', () => {
  void connect();
});
ui.disconnect.addEventListener('click', () => {
  void disconnect();
});
ui.addTopic.addEventListener('click', () => {
  void addTopic(ui.topic.value);
});
ui.topic.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') void addTopic(ui.topic.value);
});
ui.clearEvents.addEventListener('click', () => {
  ui.events.replaceChildren();
  eventCount = 0;
  ui.statEvents.textContent = '0';
  ui.clearEvents.disabled = true;
  log('cleared the stream panel');
});
ui.createNote.addEventListener('click', () => {
  createNoteFromInput();
});
ui.noteContent.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') createNoteFromInput();
});
ui.noteContent.addEventListener('input', updateCreateButton);
ui.suggestInbox.addEventListener('click', () => {
  ui.topic.value = inboxTopicOf(demoActor());
});
ui.suggestActor.addEventListener('click', () => {
  ui.topic.value = `${topicFromUri(demoActor())}/#`;
});

renderState('idle');
renderTopics([]);
renderTimeline();

if (location.protocol === 'file:') {
  // Opening the HTML directly bypasses the demo's second origin entirely.
  ui.login.disabled = true;
  log(
    'this page must be served by the demo web server: run `npm run example` and open http://localhost:5180',
  );
} else {
  log(`ready — sign in, then connect (API: ${apiLabel})`);
}
