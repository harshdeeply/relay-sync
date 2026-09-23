import test from 'node:test';
import assert from 'node:assert/strict';
import { initialState, receive, process, outage, replay, drift, reconcile, metrics } from './simulator.mjs';

test('catalog update flows into target and manual edit is repaired', () => {
  const state = initialState();
  receive(state); assert.deepEqual(process(state), [['demo-1', 'applied']]);
  assert.equal(state.target['SKU-42'].price_cents, 2249);
  drift(state); assert.deepEqual(reconcile(state), ['SKU-42']);
  assert.equal(metrics(state).drift, 1);
  reconcile(state, true); assert.deepEqual(reconcile(state), []);
});

test('outage moves event through retries and dead letter before replay', () => {
  const state = initialState();
  outage(state); receive(state);
  assert.deepEqual(process(state), [['demo-1', 'pending']]);
  assert.deepEqual(process(state), [['demo-1', 'pending']]);
  assert.deepEqual(process(state), [['demo-1', 'dead']]);
  assert.equal(metrics(state).dead, 1);
  replay(state); assert.deepEqual(process(state), [['demo-1', 'applied']]);
});

test('old version arriving after newer version cannot overwrite it', () => {
  const state = initialState();
  const old = receive(state), newer = receive(state);
  old.next_tick = 5;
  process(state);
  assert.equal(newer.status, 'applied');
  state.tick = 5;
  assert.deepEqual(process(state), [['demo-1', 'stale']]);
  assert.equal(state.target['SKU-42'].version, 2);
});
