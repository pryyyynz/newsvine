import { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { search } from '../api';
import { HeroCard, ArticleCard, WideCard } from '../components/ArticleCard';
import './SearchPage.css';

export default function SearchPage() {
  const [params] = useSearchParams();
  const q = params.get('q') || '';
  const [results, setResults] = useState(null);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState('');
  const [offset, setOffset] = useState(0);
  const limit = 20;

  useEffect(() => {
    if (q.length < 2) { setResults([]); return; }
    setError('');
    setResults(null);
    search(q, { limit, offset })
      .then((data) => {
        setResults(data.items || []);
        setTotal(data.total || 0);
      })
      .catch((e) => setError(e.message));
  }, [q, offset]);

  const articles = (results || []).map((item) => item.article);
  const topResult = articles[0];
  const nextResults = articles.slice(1, 4);
  const remaining = articles.slice(4);

  return (
    <div className="container search-page">
      <div className="section-header">
        <h2>Search results for &ldquo;{q}&rdquo;</h2>
      </div>

      {total > 0 && (
        <p className="search-page__count">{total} result{total !== 1 ? 's' : ''} found</p>
      )}

      {error && <div className="error-msg">{error}</div>}
      {!results && !error && <div className="spinner" />}

      {results && results.length === 0 && !error && (
        <p className="search-page__empty">No articles found. Try different keywords.</p>
      )}

      {articles.length > 0 && (
        <>
          {/* Top result as hero, next 3 as wide cards */}
          {offset === 0 && topResult && (
            <section className="search-page__top">
              <div className="search-page__hero">
                <HeroCard article={topResult} />
              </div>
              {nextResults.length > 0 && (
                <div className="search-page__top-list">
                  {nextResults.map((a) => (
                    <WideCard key={a.id} article={a} />
                  ))}
                </div>
              )}
            </section>
          )}

          {/* Remaining in standard grid */}
          {remaining.length > 0 && (
            <section className="search-page__rest">
              <div className="article-grid">
                {remaining.map((a) => (
                  <ArticleCard key={a.id} article={a} />
                ))}
              </div>
            </section>
          )}

          {/* Non-first pages: standard grid for all */}
          {offset > 0 && (
            <div className="article-grid">
              {articles.map((a) => (
                <ArticleCard key={a.id} article={a} />
              ))}
            </div>
          )}

          {total > limit && (
            <div className="search-page__pagination">
              <button
                disabled={offset === 0}
                onClick={() => setOffset((o) => Math.max(0, o - limit))}
              >
                ← Previous
              </button>
              <span>
                Page {Math.floor(offset / limit) + 1} of {Math.ceil(total / limit)}
              </span>
              <button
                disabled={offset + limit >= total}
                onClick={() => setOffset((o) => o + limit)}
              >
                Next →
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
