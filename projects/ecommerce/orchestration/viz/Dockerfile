FROM python:3.10-slim

WORKDIR /app

# Copy Pants lockfile and constraint extractor from monorepo root
COPY 3rdparty/user_reqs.lock 3rdparty/extract_lock_constraints.py /tmp/

# Install visualization dependencies pinned to Pants user_reqs.lock
RUN python3 /tmp/extract_lock_constraints.py --lockfile /tmp/user_reqs.lock --all-constraints > /tmp/pants-constraints.txt && \
    pip install --no-cache-dir -c /tmp/pants-constraints.txt \
    streamlit \
    duckdb \
    pandas \
    pyarrow \
    plotly && \
    rm -f /tmp/user_reqs.lock /tmp/extract_lock_constraints.py /tmp/pants-constraints.txt

COPY projects/ecommerce/orchestration/viz /app

EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
