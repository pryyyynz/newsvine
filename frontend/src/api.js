const BASE = '/api';

function authHeaders() {
  const token = localStorage.getItem('access_token');
  return token ? { Authorization: `Bearer ${token}` } : {};
}

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...authHeaders(),
      ...options.headers,
    },
  });
  if (res.status === 401) {
    const refreshed = await tryRefresh();
    if (refreshed) {
      return request(path, options);
    }
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
    window.dispatchEvent(new Event('auth-change'));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.message || err.error || res.statusText);
  }
  return res.json();
}

async function tryRefresh() {
  const refreshToken = localStorage.getItem('refresh_token');
  if (!refreshToken) return false;
  try {
    const res = await fetch(`${BASE}/auth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });
    if (!res.ok) return false;
    const data = await res.json();
    localStorage.setItem('access_token', data.access_token);
    localStorage.setItem('refresh_token', data.refresh_token);
    return true;
  } catch {
    return false;
  }
}

// Auth
export const register = (email, password, name, country) =>
  request('/auth/register', {
    method: 'POST',
    body: JSON.stringify({ email, password, name: name || undefined, country: country || undefined }),
  });

export const login = (email, password) =>
  request('/auth/login', {
    method: 'POST',
    body: JSON.stringify({ email, password }),
  });

// News
export const getNews = (params = {}) => {
  const q = new URLSearchParams();
  if (params.category) q.set('category', params.category);
  if (params.country) q.set('country', params.country);
  if (params.limit) q.set('limit', params.limit);
  if (params.offset) q.set('offset', params.offset);
  return request(`/news?${q}`);
};

export const getArticle = (id, { track = true } = {}) =>
  request(`/news/${encodeURIComponent(id)}${track ? '' : '?track=false'}`);

// Trending
export const getTrendingGlobal = (limit = 50) =>
  request(`/trending/global?limit=${limit}`);

export const getTrendingRegional = (country, limit = 50) =>
  request(`/trending/regional?user_country=${encodeURIComponent(country)}&limit=${limit}`);

// Recommendations
export const getRecommendations = (limit = 20) =>
  request(`/recommendations?limit=${limit}`);

// Search
export const search = (q, params = {}) => {
  const qs = new URLSearchParams({ q });
  if (params.country) qs.set('country', params.country);
  if (params.limit) qs.set('limit', params.limit);
  if (params.offset) qs.set('offset', params.offset);
  return request(`/search?${qs}`);
};

// Events
export const postEvent = (eventType, articleId, metadata = {}) =>
  request('/events', {
    method: 'POST',
    body: JSON.stringify({
      event_type: eventType,
      article_id: articleId,
      metadata,
    }),
  });

// User
export const getMe = () => request('/users/me');
export const updateMe = (data) =>
  request('/users/me', { method: 'PUT', body: JSON.stringify(data) });
export const getHistory = (limit = 20, offset = 0) =>
  request(`/users/me/history?limit=${limit}&offset=${offset}`);
export const getBookmarks = () => request('/users/me/bookmarks');

// Interactions
export const getArticleInteractions = (articleId) =>
  request(`/users/me/interactions/${encodeURIComponent(articleId)}`);
export const addLike = (articleId) =>
  request(`/users/me/likes/${encodeURIComponent(articleId)}`, { method: 'POST' });
export const removeLike = (articleId) =>
  request(`/users/me/likes/${encodeURIComponent(articleId)}`, { method: 'DELETE' });
export const addBookmark = (articleId) =>
  request(`/users/me/bookmarks/${encodeURIComponent(articleId)}`, { method: 'POST' });
export const removeBookmark = (articleId) =>
  request(`/users/me/bookmarks/${encodeURIComponent(articleId)}`, { method: 'DELETE' });
