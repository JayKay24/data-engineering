# Relational Transactional Storage (PostgreSQL)

This project provides a robust, containerized PostgreSQL 16 transactional storage foundation with automated DDL bootstrap, environment-driven configuration, and Python client scripts for safe transactional operations.

---

## 📁 Project Structure

```text
projects/storage/postgres/
├── docker/
│   ├── docker-compose.yml       # PostgreSQL 16-alpine container configuration
│   └── init.sql                 # Automated DDL bootstrap (transactions table & index)
├── config.py                    # Environment variable configuration & connection factory
├── insert_transactions.py       # Context-managed batch transaction insert script
├── fetch_transactions.py        # Parameterized query & verification script
├── BUILD                        # Pants python_sources and pex_binary targets
└── README.md                    # Sub-project execution documentation
```

---

## 🚀 How to Run

### 1. Start the PostgreSQL Container
From the repository root or subproject directory:
```bash
docker compose -f projects/storage/postgres/docker/docker-compose.yml up -d
```

Verify that the container is healthy:
```bash
docker ps --filter "name=local-postgres"
```

### 2. Environment Variables Configuration
The scripts connect using standard environment variables (or fall back to local development defaults):
* `POSTGRES_HOST`: `localhost`
* `POSTGRES_PORT`: `5432`
* `POSTGRES_DB`: `retail`
* `POSTGRES_USER`: `postgres`
* `POSTGRES_PASSWORD`: `mysecurepassword`

You can override these in your `.env` file or export them in your shell session.

### 3. Insert Transaction Records
Insert a batch of sample transactions:
```bash
./pants run projects/storage/postgres:insert_transactions
```

Or insert an explicit custom transaction:
```bash
./pants run projects/storage/postgres:insert_transactions -- --txn-id txn_2001 --cust-id cust_5001 --amount 149.50
```

### 4. Fetch & Validate Transactions
Query recent records:
```bash
./pants run projects/storage/postgres:fetch_transactions -- --limit 10
```

Query a specific transaction by ID:
```bash
./pants run projects/storage/postgres:fetch_transactions -- --txn-id txn_2001
```

### 5. Tear Down
To stop and remove the container:
```bash
docker compose -f projects/storage/postgres/docker/docker-compose.yml down
```
