import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../AuthContext';
import { getBookmarks, getArticle } from '../api';
import { ArticleCard } from '../components/ArticleCard';
import './SearchPage.css';

export default function BookmarksPage() {
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
    getBookmarks()
      .then(async (data) => {
        const items = data.items || [];
        const resolved = await Promise.all(
          items.map((b) => getArticle(b.article_id, { track: false }).catch(() => null))
        );
        setArticles(resolved.filter(Boolean));
      })
      .catch((e) => setError(e.message));
  }, [user]);

  if (loading || !user) return <div className="spinner" />;

  return (
    <div className="container search-page">
      <div className="section-header">
        <h2>My Bookmarks</h2>
      </div>

      {error && <div className="error-msg">{error}</div>}
      {!articles && !error && <div className="spinner" />}

      {articles && articles.length === 0 && (
        <p className="search-page__empty">
          No bookmarks yet. Click the bookmark icon on any article to save it here.
        </p>
      )}

      {articles && articles.length > 0 && (
        <div className="article-grid">
          {articles.map((a) => (
            <ArticleCard key={a.id} article={a} />
          ))}
        </div>
      )}
    </div>
  );
}
