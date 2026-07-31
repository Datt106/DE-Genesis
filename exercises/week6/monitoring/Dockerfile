FROM python:3.11-slim

WORKDIR /app

COPY exercises/week6/monitoring/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY exercises/week6/monitoring/exporter.py /app/exporter.py

EXPOSE 9108

CMD ["python", "/app/exporter.py"]
