// JSON API client. Same-origin requests carry the browser's Basic auth
// credentials automatically once the page itself was authorized.

export class ApiError extends Error {
  constructor(status, message, code) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

async function handle(res) {
  let body = null;
  const text = await res.text();
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }
  if (!res.ok) {
    const msg = (body && body.error) || text || res.statusText || `HTTP ${res.status}`;
    throw new ApiError(res.status, msg, body && body.code);
  }
  return body;
}

async function request(path, options) {
  let res;
  try {
    res = await fetch(path, options);
  } catch (e) {
    if (e.name === 'AbortError') throw e;
    throw new ApiError(0, 'the server is not reachable');
  }
  return handle(res);
}

export async function get(path, { signal } = {}) {
  return request(path, { headers: { Accept: 'application/json' }, signal, cache: 'no-store' });
}

export async function post(path, body, { signal } = {}) {
  return request(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
    signal,
  });
}

export const api = {
  overview: () => get('api/overview'),
  metrics: (since = 0) => get(`api/metrics?since=${encodeURIComponent(since)}`),
  sources: () => get('api/sources'),
  catalog: () => get('api/catalog'),
  history: (limit = 500) => get(`api/catalog/history?limit=${limit}`),
  dataflow: () => get('api/dataflow'),
  node: (id, sample = 20) => get(`api/dataflow/node/${encodeURIComponent(id)}?sample=${sample}`),
  object: (name, sample = 50) => get(`api/objects/${encodeURIComponent(name)}?sample=${sample}`),
  sessions: () => get('api/sessions'),
  errors: () => get('api/errors'),
  query: (sql, mode, limit) => post('api/query', { sql, mode, limit }),
};
