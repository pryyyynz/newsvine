import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { getTrendingGlobal } from '../api';
import { HeroCard, ArticleCard, SmallCard } from '../components/ArticleCard';
import './TrendingPage.css';

export default function TrendingPage() {
  const navigate = useNavigate();
  const [trending, setTrending] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    getTrendingGlobal(50)
      .then((data) => setTrending((data.items || []).map((i) => i.article)))
      .catch(() => setError('Failed to load trending'));
  }, []);

  if (error) return <div className="error-msg">{error}</div>;
  if (!trending) return <div className="spinner" />;

  const hero = trending[0];
  const sideCards = trending.slice(1, 4);
  const gridCards = trending.slice(4);

  return (
    <div className="container listing-page">
      <button className="listing-page__back" onClick={() => navigate(-1)}>
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="19" y1="12" x2="5" y2="12" /><polyline points="12 19 5 12 12 5" /></svg>
        Back
      </button>
      <div className="listing-page__header">
        <h1 className="listing-page__title">Trending Now</h1>
        <p className="listing-page__subtitle">The most talked-about stories right now</p>
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
