export function formatTime(ts) {
  if (!ts) return '';
  const date = new Date(ts);
  const now = new Date();
  const diffMs = now - date;
  const diffMins = Math.floor(diffMs / 60000);
  const diffHrs = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 1) return 'Just now';
  if (diffMins < 60) return `${diffMins} minute${diffMins > 1 ? 's' : ''} ago`;
  if (diffHrs < 24) return `${diffHrs} hour${diffHrs > 1 ? 's' : ''} ago`;
  if (diffDays < 7) return `${diffDays} day${diffDays > 1 ? 's' : ''} ago`;

  return date.toLocaleDateString('en-GB', {
    day: 'numeric',
    month: 'long',
    year: 'numeric',
  });
}

const _decodeEl = typeof document !== 'undefined' ? document.createElement('textarea') : null;
export function decodeEntities(text) {
  if (!text) return '';
  if (_decodeEl) {
    _decodeEl.innerHTML = text;
    return _decodeEl.value;
  }
  return text.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#x27;/g, "'").replace(/&#39;/g, "'");
}

export function truncate(text, maxLen = 180) {
  if (!text || text.length <= maxLen) return text || '';
  const decoded = decodeEntities(text);
  if (decoded.length <= maxLen) return decoded;
  return decoded.slice(0, maxLen).replace(/\s+\S*$/, '') + '…';
}

export function formatSource(source) {
  if (!source) return '';
  const map = {
    'bbc_world_rss': 'BBC News',
    'bbc_africa_rss': 'BBC Africa',
    'bbc_business_rss': 'BBC Business',
    'bbc_tech_rss': 'BBC Technology',
    'bbc_science_rss': 'BBC Science',
    'bbc_health_rss': 'BBC Health',
    'bbc_entertainment_rss': 'BBC Entertainment',
    'bbc_sport_rss': 'BBC Sport',
    'guardian_world_rss': 'The Guardian',
    'guardian_business_rss': 'The Guardian',
    'guardian_tech_rss': 'The Guardian',
    'guardian_science_rss': 'The Guardian',
    'guardian_culture_rss': 'The Guardian',
    'guardian_sport_rss': 'The Guardian',
    'ars_technica_rss': 'Ars Technica',
    'aljazeera_all_rss': 'Al Jazeera',
    'newsapi_headlines': 'NewsAPI',
  };
  return map[source] || source.replace(/_rss$/, '').replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}

export function categoryColor(cat) {
  const map = {
    business: '#0068b3',
    technology: '#5a2d82',
    science: '#00703c',
    health: '#00703c',
    entertainment: '#a64ca6',
    sports: '#d05e00',
    world: '#bb1919',
    general: '#3a3a3a',
  };
  return map[(cat || '').toLowerCase()] || '#bb1919';
}
