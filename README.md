# NERV-ML-Signals 🤖📈

ML-driven trading signal system for Binance Futures, designed with Clean Architecture (DDD) and production-ready components.

## 🌟 Overview

NERV-ML-Signals is a modular framework for developing, backtesting, and deploying algorithmic trading strategies. It leverages machine learning (CatBoost) to predict market movements using a Triple Barrier labeling method and advanced technical features.

## 🏗 Architecture

The project follows Domain-Driven Design (DDD) principles:
- **`src/domain`**: Business logic, interfaces, and core entities.
- **`src/application`**: Services orchestrating the trading flow.
- **`src/infrastructure`**: Concrete implementations (Binance API, SQLite, Telegram).
- **`etl_pipeline.py`**: Handles data ingestion, storage, and feature engineering.
- **`backtest.py`**: Realistic backtesting engine with commission and slippage simulation.
- **`run_bot.py`**: Production entry point for the live signal bot.

## 🚀 Key Features

- **Incremental ETL**: Robust data loading from Binance with local SQLite caching.
- **Feature Engineering**: 
  - Standard indicators (RSI, MACD, ATR).
  - Multi-timeframe analysis (e.g., matching 1h candles with 4h HTF trends).
  - Support/Resistance levels and price distance features.
- **ML Pipeline**: 
  - Triple Barrier method for high-quality labeling.
  - CatBoost classifier optimized for financial time series.
- **Trading Bot**: 
  - Real-time signal generation.
  - Telegram integration for instant alerts.
  - Built-in Flask server for health checks (Render-ready).

## 🛠 Setup & Installation

### Prerequisites
- Python 3.9+
- Binance API keys (for live data/trading)
- Telegram Bot Token & Chat ID

### Installation
1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd NERV-ml-signals
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure environment:
   ```bash
   cp .env.example .env
   # Edit .env with your credentials and preferences
   ```

## ⚙ Configuration

Key parameters in `.env` or `config.py`:
- `SYMBOLS`: List of trading pairs (e.g., `BTC/USDT,ETH/USDT`).
- `TIMEFRAME`: Base timeframe (e.g., `1h`).
- `CONFIDENCE_THRESHOLD`: ML prediction probability barrier.
- `RISK_PER_TRADE`: Percentage of balance to risk per trade.
- `TG_TOKEN` / `TG_CHAT_ID`: Telegram notification settings.

## 📖 Usage

### 1. Data Collection & Processing
```bash
python etl_pipeline.py
```
*Initializes DB, fetches history, and prepares features.*

### 2. Backtesting
```bash
python backtest.py
```
*Evaluates the strategy on historical data and generates an equity curve.*

### 3. Run Signal Bot
```bash
python run_bot.py
```
*Starts the live bot with Telegram notifications.*

## ☁ Deployment

The project is pre-configured for **Render.com**:
- uses `render.yaml` for infrastructure-as-code.
- `Procfile` defines the worker process.
- Built-in health check on port `8000`.

## 📜 License
[MIT](LICENSE)
