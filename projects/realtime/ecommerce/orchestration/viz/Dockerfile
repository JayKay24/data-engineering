FROM python:3.10-slim

WORKDIR /app
COPY 3rdparty/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY projects/realtime/ecommerce/orchestration/viz /app

EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
