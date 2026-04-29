import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { getNews } from '../api';
import { HeroCard, ArticleCard, WideCard, CompactCard } from '../components/ArticleCard';
import './CategoryPage.css';

export default function CategoryPage() {
  const { category } = useParams();
  const [articles, setArticles] = useState(null);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState('');
  const [offset, setOffset] = useState(0);
  const limit = 20;

  useEffect(() => {
    setOffset(0);
  }, [category]);

  useEffect(() => {
    setError('');
    setArticles(null);
    getNews({ category, limit, offset })
      .then((data) => {
        setArticles(data.items || []);
        setTotal(data.total || 0);
      })
      .catch((e) => setError(e.message));
  }, [category, offset]);

  const label = category.charAt(0).toUpperCase() + category.slice(1);

  if (error) return <div className="container search-page"><div className="error-msg">{error}</div></div>;
  if (!articles) return <div className="container search-page"><div className="spinner" /></div>;
  if (articles.length === 0) return (
    <div className="container search-page">
      <div className="section-header"><h2>{label}</h2></div>
      <p className="search-page__empty">No articles in this category yet.</p>
    </div>
  );

  const hero = articles[0];
  const featured = articles.slice(1, 5);
  const remaining = articles.slice(5);

  return (
    <div className="container category-page">
      <div className="section-header">
        <h2>{label}</h2>
        {total > 0 && <span className="category-page__count">{total} article{total !== 1 ? 's' : ''}</span>}
      </div>

      {/* Hero + featured sidebar */}
      <section className="category-page__featured">
        <div className="category-page__hero">
          <HeroCard article={hero} showTag={false} />
        </div>
        {featured.length > 0 && (
          <aside className="category-page__sidebar">
            <div className="section-header section-header--sm">
              <h3>Latest in {label}</h3>
            </div>
            {featured.map((a, i) => (
              <CompactCard key={a.id} article={a} index={i + 1} />
            ))}
          </aside>
        )}
      </section>

      {/* Remaining articles */}
      {remaining.length > 0 && (
        <section className="category-page__more">
          <div className="section-header">
            <h2>More in {label}</h2>
          </div>
          <div className="article-grid">
            {remaining.map((a) => (
              <ArticleCard key={a.id} article={a} showTag={false} />
            ))}
          </div>
        </section>
      )}

      {total > limit && (
        <div className="search-page__pagination">
          <button disabled={offset === 0} onClick={() => setOffset((o) => Math.max(0, o - limit))}>
            ← Previous
          </button>
          <span>
            Page {Math.floor(offset / limit) + 1} of {Math.ceil(total / limit)}
          </span>
          <button disabled={offset + limit >= total} onClick={() => setOffset((o) => o + limit)}>
            Next →
          </button>
        </div>
      )}
    </div>
  );
}
