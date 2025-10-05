FROM selenium/standalone-chrome:latest

# Установка Python поверх Selenium образа
USER root
RUN apt-get update && apt-get install -y python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# Даем права пользователю seluser на рабочую директорию
RUN chown -R seluser:seluser /app

# Переключаемся обратно на пользователя seluser
USER seluser

CMD ["python3", "main.py"]