FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/ services/
COPY youtrack/ youtrack/
COPY seeders/ seeders/
COPY models/ models/
COPY app.py .