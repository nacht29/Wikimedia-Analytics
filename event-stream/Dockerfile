# syntax=docker/dockerfile:1.4
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
	PYTHONIOENCODING=utf-8

WORKDIR /app/event-stream

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY src ./src
CMD ["python", "src/producer.py"]
