import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { getTrendingGlobal, getNews } from '../api';
import { HeroCard, ArticleCard, SmallCard, CompactCard } from '../components/ArticleCard';
import './TrendingPage.css';

export default function MostReadPage() {
  const navigate = useNavigate();
  const [articles, setArticles] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    getTrendingGlobal(50)
      .then((data) => {
        const items = (data.items || []).map((i) => i.article);
        if (items.length > 0) {
          setArticles(items);
        } else {
          // Fallback to latest news sorted by recency as proxy
          getNews({ limit: 50 })
            .then((n) => setArticles(n.items || []))
            .catch(() => setError('Failed to load articles'));
        }
      })
      .catch(() => setError('Failed to load most read'));
  }, []);

  if (error) return <div className="error-msg">{error}</div>;
  if (!articles) return <div className="spinner" />;

  const hero = articles[0];
  const sideCards = articles.slice(1, 4);
  const gridCards = articles.slice(4);

  return (
    <div className="container listing-page">
      <button className="listing-page__back" onClick={() => navigate(-1)}>
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="19" y1="12" x2="5" y2="12" /><polyline points="12 19 5 12 12 5" /></svg>
        Back
      </button>
      <div className="listing-page__header">
        <h1 className="listing-page__title">Most Read</h1>
        <p className="listing-page__subtitle">The stories everyone is reading</p>
      </div>

      {hero && (
        <section className="featured-grid">
          <div className="featured-grid__main">
            <HeroCard article={hero} />
          </div>
          {sideCards.length > 0 && (
            <div className="featured-grid__side">
              {sideCards.map((a) => (
                <SmallCard key={a.id} article={a} />
              ))}
            </div>
          )}
        </section>
      )}

      {gridCards.length > 0 && (
        <section className="home__section">
          <div className="article-grid">
            {gridCards.map((a) => (
              <ArticleCard key={a.id} article={a} />
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
