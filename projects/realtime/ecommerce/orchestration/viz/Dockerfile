FROM python:3.10-slim

WORKDIR /app
RUN pip install --no-cache-dir \
    streamlit>=1.38.0 \
    duckdb==1.1.0 \
    pandas>=2.0.0 \
    plotly>=5.24.0

COPY projects/realtime/ecommerce/orchestration/viz /app

EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
