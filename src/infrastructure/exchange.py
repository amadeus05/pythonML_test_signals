import requests
import logging
from typing import List
from src.domain.contracts import ExchangeInterface, KlineDTO
from config import BASE_URL

logger = logging.getLogger(__name__)

class BinanceExchange(ExchangeInterface):
    def get_latest_klines(self, symbol: str, timeframe: str, limit: int = 1500) -> List[KlineDTO]:
        """
        Fetched Klines from Binance Futures API.
        """
        api_symbol = symbol.replace("/", "")
        params = {
            "symbol": api_symbol,
            "interval": timeframe,
            "limit": limit
        }
        
        try:
            r = requests.get(BASE_URL, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            
            klines = []
            for k in data:
                klines.append(KlineDTO(
                    symbol=symbol,
                    timestamp=int(k[0]),
                    open=float(k[1]),
                    high=float(k[2]),
                    low=float(k[3]),
                    close=float(k[4]),
                    volume=float(k[5])
                ))
            return klines
        except Exception as e:
            logger.error(f"Error fetching klines for {symbol}: {e}")
            return []
