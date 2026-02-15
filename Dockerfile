# Используем Python 3.11-slim для стабильности и небольшого размера образа
FROM python:3.11-slim

# Устанавливаем системные зависимости, если понадобятся (например, для компиляции некоторых пакетов)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копируем только requirements.txt сначала для кэширования слоев
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальные файлы проекта
COPY . .

# Порт (Render использует $PORT)
EXPOSE 8000

# Запуск бота (run_bot.py запускает и логику бота, и health check сервер)
CMD ["python", "run_bot.py"]
