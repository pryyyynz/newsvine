import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    phase6_full_mix: {
      executor: 'constant-arrival-rate',
      rate: 500,
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 250,
      maxVUs: 2000,
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<500'],
    http_req_failed: ['rate<0.01'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export function setup() {
  const email = `phase6-k6-${Date.now()}@example.com`;
  const password = 'StrongPass123';

  http.post(`${BASE_URL}/auth/register`, JSON.stringify({ email, password }), {
    headers: { 'Content-Type': 'application/json' },
  });

  const login = http.post(`${BASE_URL}/auth/login`, JSON.stringify({ email, password }), {
    headers: { 'Content-Type': 'application/json' },
  });

  const token = login.status === 200 ? login.json('access_token') : '';
  return { token, userId: '1' };
}

function authHeaders(token) {
  return {
    'Content-Type': 'application/json',
    Authorization: token ? `Bearer ${token}` : '',
  };
}

export default function (data) {
  const headers = authHeaders(data.token);
  const route = Math.random();

  let res;
  if (route < 0.15) {
    res = http.get(`${BASE_URL}/news?limit=20&offset=0`, { headers });
  } else if (route < 0.30) {
    res = http.get(`${BASE_URL}/trending/global?limit=20`, { headers });
  } else if (route < 0.45) {
    res = http.get(`${BASE_URL}/search?q=technology&country=us&limit=10`, { headers });
  } else if (route < 0.60) {
    res = http.get(`${BASE_URL}/recommendations?limit=20`, { headers });
  } else if (route < 0.75) {
    res = http.post(
      `${BASE_URL}/events`,
      JSON.stringify({
        event_type: 'click',
        article_id: `k6-article-${__VU}-${__ITER}`,
        metadata: {
          user_id: `${data.userId}`,
          country: 'us',
          topic: 'tech',
        },
      }),
      { headers }
    );
  } else if (route < 0.88) {
    res = http.get(`${BASE_URL}/users/me`, { headers });
  } else {
    res = http.get(`${BASE_URL}/users/me/history?limit=20&offset=0`, { headers });
  }

  check(res, {
    'status is expected': (r) => [200, 201, 202].includes(r.status),
  });

  sleep(0.05);
}
