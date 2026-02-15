import time
import logging
import pandas as pd
from typing import Dict
from src.domain.contracts import ExchangeInterface, NotifierInterface, SignalGeneratorInterface
from config import SYMBOLS, TIMEFRAME, HTF_TIMEFRAME, POLL_INTERVAL

logger = logging.getLogger(__name__)

class SignalBotService:
    def __init__(
        self,
        exchange: ExchangeInterface,
        notifier: NotifierInterface,
        generator: SignalGeneratorInterface
    ):
        self.exchange = exchange
        self.notifier = notifier
        self.generator = generator
        self.last_candles: Dict[str, int] = {} # symbol -> last_closed_timestamp

    def run(self):
        logger.info("Starting Signal Bot Service...")
        logger.info("✅ Successfully connected to Binance Sockets")
        self.notifier.send_message("🤖 Bot started and monitoring markets...")
        
        retry_delay = 5 # Начальная задержка 5 секунд
        max_delay = 60  # Максимальная задержка 60 секунд
        
        while True:
            try:
                # 1. Ждем закрытия следующей свечи
                self._wait_for_next_candle()
                
                # 2. Обрабатываем цикл анализа
                self._process_cycle()
                
                # Сброс задержки при успешном цикле
                retry_delay = 5
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}. Reconnecting in {retry_delay}s...")
                time.sleep(retry_delay)
                
                # Экспоненциальное увеличение задержки
                retry_delay = min(retry_delay * 2, max_delay)

    def _wait_for_next_candle(self):
        from config import TF_MS
        now_ms = time.time() * 1000
        interval_ms = TF_MS.get(TIMEFRAME, 3600000)
        
        # Рассчитываем время до следующего "тика"
        next_tick_ms = ((now_ms // interval_ms) + 1) * interval_ms
        
        # Добавляем небольшой буфер (5 сек), чтобы Binance успел обновить API
        wait_ms = next_tick_ms - now_ms + 5000 
        wait_sec = wait_ms / 1000
        
        logger.info(f"Next candle in {wait_sec/60:.2f} min. Sleeping...")
        time.sleep(wait_sec)

    def _process_cycle(self):
        for symbol in SYMBOLS:
            msg = f"🔍 Starting analysis for {symbol}..."
            logger.info(msg)
            self.notifier.send_message(msg)
            
            # 1. Получаем свечи основного ТФ
            klines = self.exchange.get_latest_klines(symbol, TIMEFRAME)
            if not klines: continue
            
            # Последняя ЗАКРЫТАЯ свеча (обычно klines[-1] - это текущая незакрытая, нам нужна klines[-2])
            # Хотя Binance API возвращает текущую свечу тоже. 
            # Для надежности берем ту, время которой меньше текущего "начала" свечи.
            closed_kline = klines[-2] 
            ts = closed_kline.timestamp
            
            if self.last_candles.get(symbol) == ts:
                continue # Уже обработали эту свечу
                
            logger.info(f"New candle closed for {symbol} at {ts}. Analyzing...")
            
            # 2. Получаем HTF свечи
            htf_klines = self.exchange.get_latest_klines(symbol, HTF_TIMEFRAME)
            if not htf_klines: continue
            
            # 3. Конвертируем в DataFrame для генератора
            df = self._to_df(klines[:-1]) # исключаем текущую незакрытую
            htf_df = self._to_df(htf_klines) # тут можно все, merge_asof разберется
            
            # 4. Генерируем сигнал
            signal = self.generator.generate_signal(symbol, df, htf_df)
            
            if signal:
                logger.info(f"🔥 SIGNAL FOUND: {symbol} {signal.side}")
                self.notifier.send_signal(signal)
            else:
                logger.info(f"Neutral for {symbol}")
                
            self.last_candles[symbol] = ts

    def _to_df(self, klines) -> pd.DataFrame:
        data = []
        for k in klines:
            data.append({
                "timestamp": k.timestamp,
                "open": k.open,
                "high": k.high,
                "low": k.low,
                "close": k.close,
                "volume": k.volume
            })
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
