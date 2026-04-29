import { useEffect, useState, useRef } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { getArticle, postEvent, getArticleInteractions, addLike, removeLike, addBookmark, removeBookmark, search, getNews } from '../api';
import { useAuth } from '../AuthContext';
import { formatTime, categoryColor, formatSource, decodeEntities } from '../utils';
import { WideCard } from '../components/ArticleCard';
import './ArticlePage.css';

export default function ArticlePage() {
  const { id } = useParams();
  const { user } = useAuth();
  const navigate = useNavigate();
  const [article, setArticle] = useState(null);
  const [error, setError] = useState('');
  const [liked, setLiked] = useState(false);
  const [bookmarked, setBookmarked] = useState(false);
  const [toggling, setToggling] = useState(false);
  const [showAuthPrompt, setShowAuthPrompt] = useState(false);
  const [related, setRelated] = useState([]);
  const enteredAt = useRef(Date.now());

  useEffect(() => {
    setError('');
    setArticle(null);
    setLiked(false);
    setBookmarked(false);
    setShowAuthPrompt(false);
    enteredAt.current = Date.now();
    getArticle(id)
      .then(setArticle)
      .catch((e) => setError(e.message));
  }, [id]);

  // Track time spent on article — fire on unmount or article change
  useEffect(() => {
    const articleId = id;
    return () => {
      const seconds = Math.round((Date.now() - enteredAt.current) / 1000);
      if (seconds >= 5) {
        postEvent('time_spent', articleId, { duration_seconds: seconds }).catch(() => {});
      }
    };
  }, [id]);

  // Fetch existing interaction state when logged in
  useEffect(() => {
    if (!user) return;
    setShowAuthPrompt(false);
    getArticleInteractions(id)
      .then((data) => {
        setLiked(data.liked);
        setBookmarked(data.bookmarked);
      })
      .catch(() => {});
  }, [id, user]);

  // Fetch "You May Also Like" — search by article keywords for similar content
  useEffect(() => {
    if (!article) return;
    const stopWords = new Set(['the','a','an','is','are','was','were','in','on','at','to','for','of','and','or','but','with','by','from','as','it','its','this','that','has','have','had','be','been','not','no','so','if','do','does','did','will','can','may','how','what','who','where','when','why','all','new','one','two','vs','over','up','out','into','than','more','most','very','just','about','after','also','back','could','first','now','say','says','said','would','get','got','make','many','much','some','still','such','take','them','then','these','those','well','which','while']);
    const words = (article.title || '')
      .replace(/[^a-zA-Z0-9\s]/g, '')
      .split(/\s+/)
      .filter((w) => w.length > 2 && !stopWords.has(w.toLowerCase()));
    const query = words.slice(0, 5).join(' ');
    if (!query) return;
    search(query, { limit: 6 })
      .then((data) => {
        const items = (data.items || [])
          .map((i) => i.article)
          .filter((a) => a.id !== id)
          .slice(0, 4);
        if (items.length > 0) {
          setRelated(items);
        } else {
          getNews({ category: article.category, limit: 5 })
            .then((n) => setRelated((n.items || []).filter((a) => a.id !== id).slice(0, 4)))
            .catch(() => {});
        }
      })
      .catch(() => {
        getNews({ category: article.category, limit: 5 })
          .then((n) => setRelated((n.items || []).filter((a) => a.id !== id).slice(0, 4)))
          .catch(() => {});
      });
  }, [article, id]);

  const handleLike = async () => {
    if (!user) { setShowAuthPrompt(true); return; }
    if (toggling) return;
    setToggling(true);
    try {
      if (liked) {
        await removeLike(id);
        setLiked(false);
      } else {
        await addLike(id);
        setLiked(true);
      }
    } catch { /* ignore */ }
    setToggling(false);
  };

  const handleBookmark = async () => {
    if (!user) { setShowAuthPrompt(true); return; }
    if (toggling) return;
    setToggling(true);
    try {
      if (bookmarked) {
        await removeBookmark(id);
        setBookmarked(false);
      } else {
        await addBookmark(id);
        setBookmarked(true);
      }
    } catch { /* ignore */ }
    setToggling(false);
  };

  if (error) return <div className="error-msg">{error}</div>;
  if (!article) return <div className="spinner" />;

  const handleShare = async () => {
    const url = window.location.href;
    if (navigator.share) {
      try { await navigator.share({ title: article.title, url }); } catch { /* cancelled */ }
    } else {
      await navigator.clipboard.writeText(url);
      alert('Link copied to clipboard');
    }
  };

  return (
    <article className="container article-page">
      <div className="article-page__toolbar">
        <button className="toolbar-btn" onClick={() => navigate(-1)} aria-label="Go back">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <line x1="19" y1="12" x2="5" y2="12" />
            <polyline points="12 19 5 12 12 5" />
          </svg>
          Back
        </button>
        <button className="toolbar-btn" onClick={handleShare} aria-label="Share article">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="18" cy="5" r="3" />
            <circle cx="6" cy="12" r="3" />
            <circle cx="18" cy="19" r="3" />
            <line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
            <line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
          </svg>
          Share
        </button>
      </div>

      <div className="article-page__header">
        <span className="category-tag" style={{ background: categoryColor(article.category) }}>
          {article.category || 'News'}
        </span>
        <h1 className="article-page__title">{decodeEntities(article.title)}</h1>
        <div className="article-page__meta">
          <span className="timestamp">{formatTime(article.timestamp)}</span>
          {article.source && <span className="article-page__source">{formatSource(article.source)}</span>}
          {article.country && <span className="article-page__country">{article.country.toUpperCase()}</span>}
        </div>
      </div>

      <div className="article-page__hero">
        <img
          src={article.image_url || `https://picsum.photos/seed/${article.id}/1200/600`}
          alt=""
        />
      </div>

      <div className="article-page__grid">
        <div className="article-page__content">
          {(article.content || '').split('\n').filter(Boolean).map((p, i) => (
            <p key={i}>{decodeEntities(p)}</p>
          ))}

          {article.url && (
            <p className="article-page__original">
              <a href={article.url} target="_blank" rel="noopener noreferrer">
                Read original article →
              </a>
            </p>
          )}
        </div>

        <aside className="article-page__actions">
          <button
            className={`action-btn ${liked ? 'action-btn--active' : ''}`}
            onClick={handleLike}
            disabled={toggling}
          >
            <svg width="20" height="20" viewBox="0 0 24 24" fill={liked ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth="2">
              <path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z" />
            </svg>
            {liked ? 'Liked' : 'Like'}
          </button>
          <button
            className={`action-btn ${bookmarked ? 'action-btn--active' : ''}`}
            onClick={handleBookmark}
            disabled={toggling}
          >
            <svg width="20" height="20" viewBox="0 0 24 24" fill={bookmarked ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth="2">
              <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z" />
            </svg>
            {bookmarked ? 'Saved' : 'Bookmark'}
          </button>

          {showAuthPrompt && (
            <div className="auth-prompt">
              <p>Sign in to like and bookmark articles</p>
              <div className="auth-prompt__buttons">
                <Link to="/login" className="auth-prompt__btn">Sign In</Link>
                <Link to="/register" className="auth-prompt__btn auth-prompt__btn--secondary">Register</Link>
              </div>
              <button className="auth-prompt__dismiss" onClick={() => setShowAuthPrompt(false)}>✕</button>
            </div>
          )}
        </aside>
      </div>

      {/* Recommended articles */}
      {related.length > 0 && (
        <section className="article-page__related">
          <div className="section-header">
            <h2>You May Also Like</h2>
          </div>
          <div className="article-page__related-list">
            {related.map((a) => (
              <WideCard key={a.id} article={a} />
            ))}
          </div>
        </section>
      )}
    </article>
  );
}
