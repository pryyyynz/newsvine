import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { getTrendingGlobal, getRecommendations, getNews } from '../api';
import { useAuth } from '../AuthContext';
import { HeroCard, ArticleCard, CompactCard, WideCard, SmallCard } from '../components/ArticleCard';
import './HomePage.css';

export default function HomePage() {
  const { user } = useAuth();
  const [trending, setTrending] = useState(null);
  const [recommendations, setRecommendations] = useState(null);
  const [latest, setLatest] = useState(null);
  const [error, setError] = useState('');
  const [trendingPage, setTrendingPage] = useState(0);

  useEffect(() => {
    setError('');
    Promise.all([
      getTrendingGlobal(20).catch(() => ({ items: [] })),
      getNews({ limit: 20 }).catch(() => ({ items: [] })),
      user ? getRecommendations(10).catch(() => ({ items: [] })) : Promise.resolve(null),
    ]).then(([t, n, r]) => {
      setTrending(t.items || []);
      setLatest(n.items || []);
      setRecommendations(r ? r.items || [] : null);
    }).catch(() => setError('Failed to load content'));
  }, [user]);

  if (error) return <div className="error-msg">{error}</div>;
  if (!trending) return <div className="spinner" />;

  const hasTrending = trending.length > 0;
  const heroSource = hasTrending ? trending[0]?.article : latest?.[0];
  const featuredSide = hasTrending
    ? trending.slice(1, 4).map((t) => t.article)
    : (latest || []).slice(1, 4);
  const trendingRow = hasTrending
    ? trending.slice(4, 12).map((t) => t.article)
    : (latest || []).slice(4, 12);
  const mostRead = hasTrending
    ? trending.slice(0, 10).map((t) => t.article)
    : (latest || []).slice(0, 10);
  const latestSection = hasTrending ? latest : (latest || []).slice(7);
  const maxTrendingOffset = Math.max(trendingRow.length - 3, 0);

  return (
    <div className="container home">
      {/* Featured block: BBC-style hero + stacked side cards */}
      {heroSource && (
        <section className="featured-grid">
          <div className="featured-grid__main">
            <HeroCard article={heroSource} />
          </div>
          {featuredSide.length > 0 && (
            <div className="featured-grid__side">
              {featuredSide.map((article) => (
                <SmallCard key={article.id} article={article} />
              ))}
            </div>
          )}
        </section>
      )}

      {/* Trending row — 3-card paginated scroller */}
      {trendingRow.length > 0 && (
        <section className="home__section">
          <div className="section-header section-header--linked">
            <h2>{hasTrending ? 'Trending Now' : 'Top Stories'}</h2>
            <Link to="/trending" className="section-header__link">See all →</Link>
          </div>
          <div className="home__carousel">
            <button
              className="carousel__btn carousel__btn--prev"
              onClick={() => setTrendingPage((p) => p - 1)}
              disabled={trendingPage === 0}
              aria-label="Previous"
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="15 18 9 12 15 6" /></svg>
            </button>
            <div className="carousel__viewport">
              <div
                className="carousel__track"
                style={{ transform: `translateX(calc(-${trendingPage} * (calc((100% - 48px) / 3) + 24px)))` }}
              >
                {trendingRow.map((article) => (
                  <div className="carousel__slide" key={article.id}>
                    <ArticleCard article={article} />
                  </div>
                ))}
              </div>
            </div>
            <button
              className="carousel__btn carousel__btn--next"
              onClick={() => setTrendingPage((p) => p + 1)}
              disabled={trendingPage >= maxTrendingOffset}
              aria-label="Next"
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="9 18 15 12 9 6" /></svg>
            </button>
          </div>
        </section>
      )}

      <div className="home__grid">
        {/* Main column */}
        <div className="home__main">
          {/* For You section (authenticated) */}
          {recommendations && recommendations.length > 0 && (
            <section className="home__section">
              <div className="section-header section-header--linked">
                <h2>Recommended For You</h2>
                <Link to="/for-you" className="section-header__link">See all →</Link>
              </div>
              <div className="home__wide-list">
                {recommendations.slice(0, 5).map((item) => (
                  <WideCard key={item.article.id} article={item.article} />
                ))}
              </div>
            </section>
          )}

          {/* Latest news — standard grid */}
          {latestSection && latestSection.length > 0 && (
            <section className="home__section">
              <div className="section-header">
                <h2>Latest News</h2>
              </div>
              <div className="article-grid">
                {latestSection.map((a) => (
                  <ArticleCard key={a.id} article={a} />
                ))}
              </div>
            </section>
          )}
        </div>

        {/* Sidebar */}
        <aside className="home__sidebar">
          <div className="sidebar-block">
            <div className="section-header section-header--linked">
              <h2>Most Read</h2>
              <Link to="/most-read" className="section-header__link">See all →</Link>
            </div>
            {mostRead.map((article, i) => (
              <CompactCard key={article.id} article={article} index={i + 1} />
            ))}
          </div>
        </aside>
      </div>
    </div>
  );
}
