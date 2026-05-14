FROM python:3.12-bookworm

WORKDIR /billing_analytics_pipeline

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN pip install -e .

EXPOSE 3000

CMD ["python", "-m", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]