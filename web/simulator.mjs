export function initialState() {
  return { tick: 0, sequence: 0, inbox: [], source: {}, target: {}, audit: [], partnerFailures: 0 };
}

function log(state, eventId, action, detail = '') {
  state.audit.push({ event_id: eventId, action, detail, created_at: new Date().toISOString() });
}

export function receive(state, product = 'SKU-42') {
  const version = Math.max(0, ...state.inbox.filter(x => x.product_id === product).map(x => x.version)) + 1;
  const event = { event_id: `demo-${++state.sequence}`, product_id: product, version,
    name: 'Field kit', price_cents: 1999 + version * 250, updated_at: '2026-09-23T12:00:00Z',
    status: 'pending', attempts: 0, next_tick: state.tick };
  state.inbox.push(event);
  log(state, event.event_id, 'received', `catalog version ${version}`);
  return event;
}

export function process(state) {
  const processed = [];
  for (const event of state.inbox.filter(x => x.status === 'pending' && x.next_tick <= state.tick)) {
    const source = state.source[event.product_id];
    if (source && source.version >= event.version) {
      event.status = 'stale'; log(state, event.event_id, 'stale', `source is at v${source.version}`);
    } else if (state.partnerFailures > 0) {
      state.partnerFailures--;
      event.attempts++;
      event.status = event.attempts >= 3 ? 'dead' : 'pending';
      event.next_tick = state.tick + 1;
      log(state, event.event_id, event.status, 'fictional partner HTTP 503');
    } else {
      const row = { product_id: event.product_id, version: event.version, name: event.name,
        price_cents: event.price_cents, updated_at: event.updated_at };
      state.source[event.product_id] = row;
      state.target[event.product_id] = { ...row };
      event.status = 'applied'; log(state, event.event_id, 'applied', `v${event.version}`);
    }
    processed.push([event.event_id, event.status]);
  }
  state.tick++;
  return processed;
}

export function outage(state) {
  state.partnerFailures = 3;
  log(state, 'partner', 'outage', 'next three attempts return 503');
}

export function replay(state) {
  const event = state.inbox.find(x => x.status === 'dead');
  if (!event) throw new Error('There is no dead letter to replay');
  event.status = 'pending'; event.attempts = 0; event.next_tick = state.tick;
  state.partnerFailures = 0;
  log(state, event.event_id, 'replayed');
  return event.event_id;
}

export function drift(state) {
  const target = state.target['SKU-42'];
  if (!target) throw new Error('Process an event before simulating drift');
  target.price_cents = 1;
  log(state, 'SKU-42', 'target_edit', 'price changed outside the catalog');
}

export function reconcile(state, repair = false) {
  const drifted = Object.values(state.source).filter(row => {
    const target = state.target[row.product_id];
    return !target || ['version', 'name', 'price_cents'].some(key => row[key] !== target[key]);
  });
  if (repair) for (const row of drifted) {
    state.target[row.product_id] = { ...row };
    log(state, row.product_id, 'reconciled', `restored source v${row.version}`);
  }
  return drifted.map(x => x.product_id);
}

export function metrics(state) {
  return { products: Object.keys(state.source).length,
    pending: state.inbox.filter(x => x.status === 'pending').length,
    dead: state.inbox.filter(x => x.status === 'dead').length,
    drift: reconcile(state).length };
}
