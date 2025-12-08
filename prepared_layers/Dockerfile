FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . prepared_layers/
ENV PYTHONPATH=/app
CMD ["python", "-u", "prepared_layers/main.py"]
