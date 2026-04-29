import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    event_spike: {
      executor: 'constant-vus',
      vus: 1000,
      duration: '1m',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<100'],
    http_req_failed: ['rate<0.01'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export default function () {
  const payload = JSON.stringify({
    event_type: 'click',
    article_id: `k6-article-${__VU}-${__ITER}`,
    metadata: {
      user_id: `k6-user-${__VU}`,
      country: 'us',
      topic: 'tech',
    },
  });

  const res = http.post(`${BASE_URL}/events`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 202': (r) => r.status === 202,
  });

  sleep(0.1);
}
