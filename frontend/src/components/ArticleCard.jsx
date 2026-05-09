import { Link } from 'react-router-dom';
import { formatTime, truncate, categoryColor, formatSource, decodeEntities } from '../utils';
import './ArticleCard.css';

function escapeSvgText(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

export function articlePlaceholderImage(article, w, h) {
  const category = escapeSvgText((article?.category || 'News').toUpperCase());
  const title = escapeSvgText(decodeEntities(article?.title || 'Newsvine').slice(0, 54));
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
      <rect width="100%" height="100%" fill="#f1f3f4"/>
      <rect x="0" y="0" width="100%" height="8" fill="${categoryColor(article?.category || 'general')}"/>
      <text x="24" y="${Math.round(h * 0.48)}" fill="#bb1919" font-family="Arial, sans-serif" font-size="${Math.max(18, Math.round(w / 12))}" font-weight="800">NV</text>
      <text x="24" y="${Math.round(h * 0.66)}" fill="#333333" font-family="Arial, sans-serif" font-size="${Math.max(12, Math.round(w / 26))}" font-weight="700">${category}</text>
      <text x="24" y="${Math.round(h * 0.8)}" fill="#666666" font-family="Arial, sans-serif" font-size="${Math.max(10, Math.round(w / 34))}">${title}</text>
    </svg>
  `;
  return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
}

export function articleImg(article, w, h) {
  return article?.image_url?.trim() || articlePlaceholderImage(article, w, h);
}

function imageFallbackHandler(article, w, h) {
  return (event) => {
    event.currentTarget.onerror = null;
    event.currentTarget.src = articlePlaceholderImage(article, w, h);
  };
}

export function HeroCard({ article, showTag = true }) {
  return (
    <Link to={`/article/${encodeURIComponent(article.id)}`} className="hero-card">
      <div className="hero-card__img">
        <img
          src={articleImg(article, 960, 480)}
          alt=""
          loading="eager"
          onError={imageFallbackHandler(article, 960, 480)}
        />
      </div>
      <div className="hero-card__body">
        {showTag && (
          <span className="category-tag" style={{ background: categoryColor(article.category) }}>
            {article.category || 'News'}
          </span>
        )}
        <h1 className="hero-card__title">{decodeEntities(article.title)}</h1>
        <p className="hero-card__snippet">{truncate(article.content_snippet || article.content, 260)}</p>
        <div className="hero-card__meta">
          <span className="timestamp">{formatTime(article.timestamp)}</span>
          {article.source && <span className="hero-card__source">{formatSource(article.source)}</span>}
        </div>
      </div>
    </Link>
  );
}

export function ArticleCard({ article, rank, showTag = true, timePrefix }) {
  return (
    <Link to={`/article/${encodeURIComponent(article.id)}`} className="article-card">
      <div className="article-card__img">
        <img
          src={articleImg(article, 400, 240)}
          alt=""
          loading="lazy"
          onError={imageFallbackHandler(article, 400, 240)}
        />
      </div>
      <div className="article-card__body">
        {showTag && (
          <span className="category-tag" style={{ background: categoryColor(article.category) }}>
            {article.category || 'News'}
          </span>
        )}
        <h3 className="article-card__title">{decodeEntities(article.title)}</h3>
        <p className="article-card__snippet">{truncate(article.content_snippet || article.content, 120)}</p>
        <span className="timestamp">{timePrefix ? `${timePrefix} ${formatTime(article.timestamp).toLowerCase()}` : formatTime(article.timestamp)}</span>
      </div>
      {rank != null && <span className="article-card__rank">{rank}</span>}
    </Link>
  );
}

export function CompactCard({ article, index }) {
  return (
    <Link to={`/article/${encodeURIComponent(article.id)}`} className="compact-card">
      <span className="compact-card__index">{index}</span>
      <div className="compact-card__body">
        <h4 className="compact-card__title">{decodeEntities(article.title)}</h4>
        <span className="timestamp">{formatTime(article.timestamp)}</span>
      </div>
    </Link>
  );
}

export function WideCard({ article, showTag = true, timePrefix }) {
  return (
    <Link to={`/article/${encodeURIComponent(article.id)}`} className="wide-card">
      <div className="wide-card__img">
        <img
          src={articleImg(article, 480, 320)}
          alt=""
          loading="lazy"
          onError={imageFallbackHandler(article, 480, 320)}
        />
      </div>
      <div className="wide-card__body">
        {showTag && (
          <span className="category-tag" style={{ background: categoryColor(article.category) }}>
            {article.category || 'News'}
          </span>
        )}
        <h3 className="wide-card__title">{decodeEntities(article.title)}</h3>
        <p className="wide-card__snippet">{truncate(article.content_snippet || article.content, 160)}</p>
        <span className="timestamp">{timePrefix ? `${timePrefix} ${formatTime(article.timestamp).toLowerCase()}` : formatTime(article.timestamp)}</span>
      </div>
    </Link>
  );
}

export function SmallCard({ article, showTag = true }) {
  return (
    <Link to={`/article/${encodeURIComponent(article.id)}`} className="small-card">
      <div className="small-card__img">
        <img
          src={articleImg(article, 200, 200)}
          alt=""
          loading="lazy"
          onError={imageFallbackHandler(article, 200, 200)}
        />
      </div>
      <div className="small-card__body">
        {showTag && (
          <span className="category-tag" style={{ background: categoryColor(article.category) }}>
            {article.category || 'News'}
          </span>
        )}
        <h4 className="small-card__title">{decodeEntities(article.title)}</h4>
        <span className="timestamp">{formatTime(article.timestamp)}</span>
      </div>
    </Link>
  );
}
