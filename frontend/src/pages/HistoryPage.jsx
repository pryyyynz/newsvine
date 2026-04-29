import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../AuthContext';
import { getHistory } from '../api';
import { WideCard, ArticleCard } from '../components/ArticleCard';
import './HistoryPage.css';

export default function HistoryPage() {
  const { user, loading } = useAuth();
  const navigate = useNavigate();
  const [articles, setArticles] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!loading && !user) navigate('/login');
  }, [user, loading, navigate]);

  useEffect(() => {
    if (!user) return;
    setError('');
    getHistory(200)
      .then((data) => {
        const items = data.items || [];
        // Deduplicate by article_id, keeping the most recent view
        const seen = new Set();
        const unique = items.filter((h) => {
          if (!h.article || seen.has(h.article_id)) return false;
          seen.add(h.article_id);
          return true;
        });
        setArticles(unique.map((h) => ({ ...h.article, timestamp: h.read_at })));
      })
      .catch((e) => setError(e.message));
  }, [user]);

  if (loading || !user) return <div className="spinner" />;

  const recent = (articles || []).slice(0, 3);
  const older = (articles || []).slice(3);

  return (
    <div className="container history-page">
      <div className="section-header">
        <h2>Reading History</h2>
      </div>

      {error && <div className="error-msg">{error}</div>}
      {!articles && !error && <div className="spinner" />}

      {articles && articles.length === 0 && (
        <p className="search-page__empty">
          No reading history yet. Articles you read will appear here.
        </p>
      )}

      {/* Recent reads — wide cards */}
      {recent.length > 0 && (
        <section className="history-page__section">
          <div className="section-header section-header--sm">
            <h3>Recently Read</h3>
          </div>
          <div className="history-page__recent">
            {recent.map((a) => (
              <WideCard key={a.id} article={a} timePrefix="Read" />
            ))}
          </div>
        </section>
      )}

      {/* Older reads — standard grid */}
      {older.length > 0 && (
        <section className="history-page__section">
          <div className="section-header section-header--sm">
            <h3>Earlier</h3>
          </div>
          <div className="article-grid">
            {older.map((a) => (
              <ArticleCard key={a.id} article={a} timePrefix="Read" />
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
