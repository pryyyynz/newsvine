import { useEffect, useState } from 'react';
import './Ticker.css';

const SYMBOLS = [
  'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT',
  'ADAUSDT', 'DOGEUSDT', 'AVAXUSDT', 'DOTUSDT', 'MATICUSDT',
  'LINKUSDT', 'LTCUSDT', 'ATOMUSDT', 'UNIUSDT', 'NEARUSDT',
];

const NAMES = {
  BTCUSDT: 'Bitcoin', ETHUSDT: 'Ethereum', BNBUSDT: 'BNB', SOLUSDT: 'Solana',
  XRPUSDT: 'XRP', ADAUSDT: 'Cardano', DOGEUSDT: 'Doge', AVAXUSDT: 'Avalanche',
  DOTUSDT: 'Polkadot', MATICUSDT: 'Polygon', LINKUSDT: 'Chainlink',
  LTCUSDT: 'Litecoin', ATOMUSDT: 'Cosmos', UNIUSDT: 'Uniswap', NEARUSDT: 'NEAR',
};

function Sparkline({ data, isUp, width = 50, height = 20 }) {
  if (!data || data.length < 2) return null;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const step = width / (data.length - 1);
  const points = data.map((v, i) => `${(i * step).toFixed(1)},${(height - ((v - min) / range) * height).toFixed(1)}`).join(' ');
  const color = isUp ? '#0ecb81' : '#f6465d';
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} className="ticker__spark">
      <polyline fill="none" stroke={color} strokeWidth="1.5" points={points} />
    </svg>
  );
}

export default function Ticker() {
  const [coins, setCoins] = useState([]);
  const [klines, setKlines] = useState({});

  useEffect(() => {
    const fetchPrices = () => {
      const query = JSON.stringify(SYMBOLS);
      fetch(`https://api.binance.com/api/v3/ticker/24hr?symbols=${encodeURIComponent(query)}`)
        .then((r) => r.json())
        .then((data) => {
          if (Array.isArray(data)) {
            const ordered = SYMBOLS.map((s) => data.find((d) => d.symbol === s)).filter(Boolean);
            setCoins(ordered);
          }
        })
        .catch(() => {});
    };
    fetchPrices();
    const interval = setInterval(fetchPrices, 30000);
    return () => clearInterval(interval);
  }, []);

  // Fetch 24h kline data for sparklines
  useEffect(() => {
    SYMBOLS.forEach((sym) => {
      fetch(`https://api.binance.com/api/v3/klines?symbol=${sym}&interval=1h&limit=24`)
        .then((r) => r.json())
        .then((data) => {
          if (Array.isArray(data)) {
            setKlines((prev) => ({ ...prev, [sym]: data.map((k) => parseFloat(k[4])) }));
          }
        })
        .catch(() => {});
    });
  }, []);

  if (coins.length === 0) return null;

  const doubled = [...coins, ...coins];

  return (
    <div className="ticker">
      <div className="ticker__label">
        <span className="ticker__live-dot" />
        MARKETS
      </div>
      <div className="ticker__track">
        <div className="ticker__scroll">
          {doubled.map((coin, i) => {
            const price = parseFloat(coin.lastPrice);
            const change = parseFloat(coin.priceChangePercent);
            const symbol = coin.symbol.replace('USDT', '');
            const isUp = change >= 0;
            const sparkData = klines[coin.symbol];
            return (
              <a
                key={`${coin.symbol}-${i}`}
                className="ticker__item"
                href={`https://www.binance.com/en/trade/${symbol}_USDT`}
                target="_blank"
                rel="noopener noreferrer"
              >
                <img
                  className="ticker__coin-icon"
                  src={`https://cdn.jsdelivr.net/gh/spothq/cryptocurrency-icons@master/128/color/${symbol.toLowerCase()}.png`}
                  alt={symbol}
                  width="18"
                  height="18"
                  loading="lazy"
                  onError={(e) => { e.target.style.display = 'none'; }}
                />
                <span className="ticker__coin-name">{NAMES[coin.symbol] || symbol}</span>
                <span className="ticker__coin-symbol">{symbol}</span>
                <span className="ticker__price">
                  ${price >= 1 ? price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : price.toPrecision(4)}
                </span>
                <Sparkline data={sparkData} isUp={isUp} />
                <span className={`ticker__change ${isUp ? 'ticker__change--up' : 'ticker__change--down'}`}>
                  {isUp ? '▲' : '▼'} {Math.abs(change).toFixed(2)}%
                </span>
              </a>
            );
          })}
        </div>
      </div>
    </div>
  );
}
