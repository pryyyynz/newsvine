import { useState, useCallback } from 'react';
import { Link, Outlet, useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../AuthContext';
import Ticker from './Ticker';
import './Layout.css';

const CATEGORIES = [
  'World', 'Business', 'Technology', 'Science', 'Health',
  'Entertainment', 'Sports',
];

export default function Layout() {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [searchQuery, setSearchQuery] = useState('');
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const isActive = (path) => {
    if (path === '/') return location.pathname === '/';
    if (path === '/for-you') return location.pathname === '/for-you';
    return location.pathname.startsWith(path);
  };

  const handleSearch = useCallback(
    (e) => {
      e.preventDefault();
      const q = searchQuery.trim();
      if (q.length >= 2) {
        navigate(`/search?q=${encodeURIComponent(q)}`);
        setSearchQuery('');
        setMobileMenuOpen(false);
      }
    },
    [searchQuery, navigate],
  );

  return (
    <>
      {/* Top bar */}
      <header className="header">
        <div className="container header__inner">
          <Link to="/" className="header__logo">
            <span className="header__logo-icon">NV</span>
            <span className="header__logo-text">NewsVine</span>
          </Link>

          <form className="header__search" onSubmit={handleSearch}>
            <input
              type="search"
              placeholder="Search news…"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="header__search-input"
              aria-label="Search news"
              minLength={2}
            />
            <button type="submit" className="header__search-btn" aria-label="Search">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                <circle cx="11" cy="11" r="7" />
                <line x1="16.5" y1="16.5" x2="21" y2="21" />
              </svg>
            </button>
          </form>

          <div className="header__actions">
            {user ? (
              <div className="header__user-menu">
                <Link to="/profile" className="header__user-link">
                  <span className="header__avatar">{user.email[0].toUpperCase()}</span>
                </Link>
                <button onClick={logout} className="header__btn header__btn--sign-out">
                  Sign Out
                </button>
              </div>
            ) : (
              <>
                <Link to="/login" className="header__btn">Sign In</Link>
                <Link to="/register" className="header__btn header__btn--register">Register</Link>
              </>
            )}
          </div>

          <button
            className="header__hamburger"
            onClick={() => setMobileMenuOpen((o) => !o)}
            aria-label="Menu"
          >
            <span />
            <span />
            <span />
          </button>
        </div>
      </header>

      {/* Category nav */}
      <nav className={`nav ${mobileMenuOpen ? 'nav--open' : ''}`}>
        <div className="container nav__inner">
          <ul className="nav__list">
            <li>
              <Link to="/" className={`nav__link${isActive('/') ? ' nav__link--active' : ''}`} onClick={() => setMobileMenuOpen(false)}>Home</Link>
            </li>
            {user && (
              <li>
                <Link to="/for-you" className={`nav__link${isActive('/for-you') ? ' nav__link--active' : ''}`} onClick={() => setMobileMenuOpen(false)}>For You</Link>
              </li>
            )}
            {CATEGORIES.map((cat) => (
              <li key={cat}>
                <Link
                  to={`/category/${cat.toLowerCase()}`}
                  className={`nav__link${isActive(`/category/${cat.toLowerCase()}`) ? ' nav__link--active' : ''}`}
                  onClick={() => setMobileMenuOpen(false)}
                >
                  {cat}
                </Link>
              </li>
            ))}
            {user && (
              <>
                <li className="nav__divider" />
                <li>
                  <Link to="/bookmarks" className={`nav__link${isActive('/bookmarks') ? ' nav__link--active' : ''}`} onClick={() => setMobileMenuOpen(false)}>Bookmarks</Link>
                </li>
                <li>
                  <Link to="/history" className={`nav__link${isActive('/history') ? ' nav__link--active' : ''}`} onClick={() => setMobileMenuOpen(false)}>History</Link>
                </li>
              </>
            )}
          </ul>
        </div>
      </nav>

      <Ticker />

      <main className="main">
        <Outlet />
      </main>

      <footer className="footer">
        <div className="container footer__inner">
          <div className="footer__brand">
            <span className="footer__logo">NV</span>
            <span>NewsVine &copy; {new Date().getFullYear()}</span>
          </div>
          <div className="footer__links">
            <Link to="/">Home</Link>
            <Link to="/search?q=latest">Search</Link>
            {user && <Link to="/profile">Profile</Link>}
          </div>
        </div>
      </footer>
    </>
  );
}
