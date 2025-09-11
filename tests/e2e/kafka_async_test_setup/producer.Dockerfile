FROM python:3.10-slim

WORKDIR /app

RUN pip install confluent-kafka

COPY producer_script.py /app/

CMD ["python", "producer_script.py"]