import { initialState, receive, process, outage, replay, drift, reconcile, metrics } from './simulator.mjs';

const $ = id => document.getElementById(id);
const escape = value => String(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
let state;
try { state = JSON.parse(localStorage.getItem('relay-demo-v1')) || initialState(); }
catch { state = initialState(); }

function render() {
  const m = metrics(state);
  for (const key of ['products', 'pending', 'dead', 'drift']) $(key).textContent = m[key];
  $('mirror').innerHTML = Object.values(state.source).map(row => {
    const target = state.target[row.product_id];
    return `<tr><td class="mono">${escape(row.product_id)}</td><td>${row.version}</td><td>$${(row.price_cents / 100).toFixed(2)}</td><td>$${((target?.price_cents || 0) / 100).toFixed(2)}</td></tr>`;
  }).join('') || '<tr><td colspan="4">Generate an event, then process it</td></tr>';
  $('inbox').innerHTML = state.inbox.slice(-8).reverse().map(row => `<tr><td class="mono">${escape(row.event_id)}</td><td><span class="pill">${escape(row.status)}</span></td><td>${row.attempts}</td></tr>`).join('') || '<tr><td colspan="3">No events yet</td></tr>';
  $('log').textContent = state.audit.slice(-12).reverse().map(row => `${row.action.padEnd(12)} ${row.event_id} ${row.detail}`).join('\n') || 'Generate an event to begin.';
  localStorage.setItem('relay-demo-v1', JSON.stringify(state));
}

document.querySelectorAll('[data-action]').forEach(button => button.addEventListener('click', () => {
  const actions = { receive, process, outage, replay, drift, repair: s => reconcile(s, true), reset: () => { state = initialState(); } };
  try { actions[button.dataset.action](state); render(); }
  catch (error) { $('log').textContent = error.message; }
}));
render();
