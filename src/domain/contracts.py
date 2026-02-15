from dataclasses import dataclass
from enum import Enum
from typing import List, Optional
from abc import ABC, abstractmethod
import pandas as pd

class SignalSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"

@dataclass
class SignalDTO:
    symbol: str
    side: SignalSide
    confidence: float
    current_price: float
    take_profit: float
    stop_loss: float
    expected_move_pct: float

@dataclass
class KlineDTO:
    symbol: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

class NotifierInterface(ABC):
    @abstractmethod
    def send_signal(self, signal: SignalDTO):
        pass

    @abstractmethod
    def send_message(self, message: str):
        pass

class ExchangeInterface(ABC):
    @abstractmethod
    def get_latest_klines(self, symbol: str, timeframe: str, limit: int = 200) -> List[KlineDTO]:
        pass

class SignalGeneratorInterface(ABC):
    @abstractmethod
    def generate_signal(self, symbol: str, klines_df: pd.DataFrame, htf_klines_df: pd.DataFrame) -> Optional[SignalDTO]:
        pass
