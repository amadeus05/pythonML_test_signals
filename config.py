import os
from pathlib import Path

# --- ОСНОВНЫЕ ---
DB_PATH = os.getenv("DB_PATH", "market_data.db")
SYMBOLS_RAW = os.getenv("SYMBOLS", "ETH/USDT")
SYMBOLS = [s.strip() for s in SYMBOLS_RAW.split(",") if s.strip()]

TIMEFRAME = os.getenv("TIMEFRAME", "1h")
HTF_TIMEFRAME = os.getenv("HTF_TIMEFRAME", "4h")

# --- DATA LOADING ---
START_DATE = "2022-01-01"
END_DATE = None
BINANCE_LIMIT = 1500
BINANCE_SLEEP = 0.3

# --- ML LABELING ---
HORIZON = 12
ATR_MULTIPLIER = 2.0

# --- TRADING / BOT ---
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", 0.65))
TP_PCT = float(os.getenv("TP_PCT", 0.030))
SL_PCT = float(os.getenv("SL_PCT", 0.015))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", 0.01))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 10))

# Интервалы в миллисекундах
TF_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}

# --- TELEGRAM ---
TG_TOKEN = os.getenv("TG_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

# --- PATHS ---
MODELS_DIR = Path("models")
MODELS_DIR.mkdir(exist_ok=True)

BASE_URL = "https://fapi.binance.com/fapi/v1/klines"