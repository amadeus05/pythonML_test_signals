import logging
import threading
from flask import Flask
from dotenv import load_dotenv
import os

load_dotenv() # Load environment variables from .env file

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

import time
from datetime import datetime, timedelta

# --- Render Health Check Server ---
app = Flask(__name__)
START_TIME = datetime.now()

@app.route('/')
def health_check():
    uptime = datetime.now() - START_TIME
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    status = (
        f"🤖 Bot is running!<br>"
        f"Uptime: {days}d {hours}h {minutes}m {seconds}s<br>"
        f"Started at: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    return status, 200

def run_web():
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port)

# --- Main Bot Execution ---
def main():
    # Dependency Injection
    exchange = BinanceExchange()
    notifier = TelegramNotifier()
    generator = MLSignalGenerator()
    
    bot_service = SignalBotService(exchange, notifier, generator)
    
    # Start web server in background for Render
    threading.Thread(target=run_web, daemon=True).start()
    
    # Start Bot
    bot_service.run()

if __name__ == "__main__":
    main()
