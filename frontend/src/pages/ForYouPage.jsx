import { useEffect, useState } from 'react';
import { getRecommendations, getNews, getTrendingRegional } from '../api';
import { useAuth } from '../AuthContext';
import { HeroCard, ArticleCard, WideCard, SmallCard, CompactCard } from '../components/ArticleCard';
import './ForYouPage.css';

export default function ForYouPage() {
  const { user } = useAuth();
  const [recommendations, setRecommendations] = useState(null);
  const [regional, setRegional] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!user) return;
    setError('');

    // Fetch recommendations
    getRecommendations(20)
      .then((data) => {
        const items = (data.items || []).map((i) => i.article);
        setRecommendations(items);

        // If few recommendations (new user), supplement with country-based or trending news
        if (items.length < 5 && user.country) {
          getTrendingRegional(user.country, 20)
            .then((t) => setRegional((t.items || []).map((i) => i.article)))
            .catch(() => {
              getNews({ country: user.country, limit: 20 })
                .then((n) => setRegional(n.items || []))
                .catch(() => {});
            });
        } else if (items.length < 5) {
          getNews({ limit: 20 })
            .then((n) => setRegional(n.items || []))
            .catch(() => {});
        }
      })
      .catch(() => {
        setRecommendations([]);
        // Fallback to country/region news
        if (user.country) {
          getTrendingRegional(user.country, 20)
            .then((t) => setRegional((t.items || []).map((i) => i.article)))
            .catch(() => {
              getNews({ country: user.country, limit: 20 })
                .then((n) => setRegional(n.items || []))
                .catch(() => {});
            });
        } else {
          getNews({ limit: 20 })
            .then((n) => setRegional(n.items || []))
            .catch(() => {});
        }
      });
  }, [user]);

  if (!user) return null;
  if (!recommendations) return <div className="spinner" />;
  if (error) return <div className="error-msg">{error}</div>;

  const hasRecs = recommendations.length > 0;
  const hero = hasRecs ? recommendations[0] : regional?.[0];
  const sideCards = hasRecs ? recommendations.slice(1, 4) : (regional || []).slice(1, 4);
  const gridCards = hasRecs ? recommendations.slice(4) : (regional || []).slice(4);
  const showRegional = regional && regional.length > 0 && hasRecs;

  return (
    <div className="container for-you">
      <div className="for-you__header">
        <h1 className="for-you__title">For You</h1>
        <p className="for-you__subtitle">
          {hasRecs
            ? 'Personalised picks based on your reading habits'
            : user.country
              ? `Trending stories from ${user.country.toUpperCase()}`
              : 'Popular stories to get you started'}
        </p>
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
          <div className="section-header">
            <h2>{hasRecs ? 'More For You' : 'Recommended'}</h2>
          </div>
          <div className="article-grid">
            {gridCards.map((a) => (
              <ArticleCard key={a.id} article={a} />
            ))}
          </div>
        </section>
      )}

      {showRegional && (
        <section className="home__section">
          <div className="section-header">
            <h2>From Your Region</h2>
          </div>
          <div className="article-grid">
            {regional.slice(0, 6).map((a) => (
              <ArticleCard key={a.id} article={a} />
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
