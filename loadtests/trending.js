import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    trending_read: {
      executor: 'constant-vus',
      vus: 300,
      duration: '1m',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<50'],
    http_req_failed: ['rate<0.01'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export default function () {
  const res = http.get(`${BASE_URL}/trending/global?limit=50`);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  sleep(0.1);
}
