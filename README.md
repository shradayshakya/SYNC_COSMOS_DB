# 🚀 Cosmos DB Migration Tool

A robust Python utility to migrate data between two Azure Cosmos DB accounts, including full database and container copying, partition key validation, and safe deduplication using insert conflict handling.

## 📦 Features

- 🔁 Migrate all databases and containers from source to target Cosmos DB accounts
- 🔐 Enforces strict partition key path alignment

## ⚙️ Setup Instructions

### 1. Set up Python environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp env.example .env
```

Edit .env and set your Cosmos DB credentials:

```dotenv
SOURCE_ACCOUNT=your-source-account
TARGET_ACCOUNT=your-target-account
SOURCE_KEY=your-source-key
TARGET_KEY=your-target-key
```

## 🚀 Running the Migration

### Full migration (all databases and containers):

```bash
python migrate.py
```
