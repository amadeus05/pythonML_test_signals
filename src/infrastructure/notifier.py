import requests
import logging
from src.domain.contracts import NotifierInterface, SignalDTO, SignalSide
from config import TG_TOKEN, TG_CHAT_ID

logger = logging.getLogger(__name__)

class TelegramNotifier(NotifierInterface):
    def __init__(self):
        self.base_url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

    def send_message(self, text: str):
        params = {
            "chat_id": TG_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }
        try:
            r = requests.get(self.base_url, params=params, timeout=10)
            r.raise_for_status()
        except Exception as e:
            logger.error(f"Error sending TG message: {e}")

    def send_signal(self, signal: SignalDTO):
        emoji = "🟢 LONG" if signal.side == SignalSide.LONG else "🔴 SHORT"
        
        message = (
            f"🚀 *NEW SIGNAL: {signal.symbol}*\n"
            f"Direction: {emoji}\n"
            f"Confidence: `{signal.confidence:.2%}`\n"
            f"Current Price: `{signal.current_price:.4f}`\n"
            f"-------------------\n"
            f"🎯 *TAKE PROFIT*: `{signal.take_profit:.4f}`\n"
            f"🛑 *STOP LOSS*: `{signal.stop_loss:.4f}`\n"
            f"📈 *Expected Move*: `{signal.expected_move_pct:.2%}`"
        )
        self.send_message(message)
