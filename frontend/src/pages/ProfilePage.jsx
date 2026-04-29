import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../AuthContext';
import { updateMe } from '../api';
import COUNTRIES from '../countries';
import './ProfilePage.css';

export default function ProfilePage() {
  const { user, loading } = useAuth();
  const navigate = useNavigate();
  const [country, setCountry] = useState('');
  const [name, setName] = useState('');
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    if (!loading && !user) navigate('/login');
    if (user) {
      setCountry(user.country || '');
      setName(user.name || '');
    }
  }, [user, loading, navigate]);

  const handleSave = async (e) => {
    e.preventDefault();
    setSaving(true);
    setSaved(false);
    try {
      await updateMe({ name: name || undefined, country: country || undefined });
      setSaved(true);
      window.dispatchEvent(new Event('auth-change'));
    } catch { /* ignore */ }
    setSaving(false);
  };

  if (loading || !user) return <div className="spinner" />;

  return (
    <div className="container profile-page">
      <div className="section-header">
        <h2>My Profile</h2>
      </div>

      <div className="profile-card">
        <div className="profile-card__avatar">
          {(user.name || user.email)[0].toUpperCase()}
        </div>
        <div className="profile-card__info">
          <h3>{user.name || user.email}</h3>
          {user.name && <span className="timestamp">{user.email}</span>}
          <span className="timestamp">User ID: {user.id}</span>
        </div>
      </div>

      <form onSubmit={handleSave} className="profile-form">
        <div className="auth-form__field">
          <label htmlFor="name">Display name</label>
          <input
            id="name"
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="Your display name"
          />
        </div>
        <div className="auth-form__field">
          <label htmlFor="country">Country / Region</label>
          <select
            id="country"
            value={country}
            onChange={(e) => setCountry(e.target.value)}
          >
            {COUNTRIES.map((c) => (
              <option key={c.code} value={c.code}>{c.name}</option>
            ))}
          </select>
          <span className="profile-form__hint">
            Set your country to see regional trending news
          </span>
        </div>
        <button type="submit" className="auth-form__submit" disabled={saving}>
          {saving ? 'Saving…' : 'Save Changes'}
        </button>
        {saved && <p className="profile-form__success">Profile updated successfully</p>}
      </form>
    </div>
  );
}
