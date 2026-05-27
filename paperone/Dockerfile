FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends jq wget && rm -rf /var/lib/apt/lists/*

COPY create-token.sh .

RUN chmod +x create-token.sh

COPY services/ services/
COPY youtrack/ youtrack/
COPY models/ models/
COPY app.py .

EXPOSE 8000

CMD ["sh", "-c", "./create-token.sh && python app.py"]
