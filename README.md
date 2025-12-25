Kasparro Crypto Data Pipeline:

-A production ready ETL and API system that ingests cryptocurrency market data from multiple sources, unifies schemas, stores it in PostgreSQL, and exposes it via a FastAPI service. Fully Dockerized with observability and rate limiting.

----------------------------------------------------------------------------------------------------
Tech Stack:

-Python 3.11

-FastAPI

-PostgreSQL

-SQLAlchemy

-Docker & Docker Compose

-Requests

----------------------------------------------------------------------------------------------------
Data Sources:

-P0.1 — Two Sources

-API Source: CoinGecko (live crypto market data)

-CSV Source: Structured price dataset

-P1.1 — Third Source

-CSV (Quirky Format): Prices with currency symbols, commas, and inconsistent formatting

-All sources are ingested into raw tables and unified into a single coins table.

----------------------------------------------------------------------------------------------------
Architecture Overview

-ETL Flow:

API / CSV Sources

      ↓
      
Raw Tables (raw_coingecko, raw_csv, raw_csv_quirky)

      ↓
      
Schema Unification Layer

      ↓
      
Unified Table (coins)



-API Layer:

/data → Query unified coin data

/stats → ETL run metadata & observability

----------------------------------------------------------------------------------------------------
Database Design

Core Tables:

raw_coingecko – Raw API payloads

raw_csv_prices – Clean CSV data

raw_csv_quirky – CSV with formatting issues

coins – Unified, queryable dataset

etl_runs – ETL execution tracking (status, timestamps)

----------------------------------------------------------------------------------------------------
Features by Priority

P0 — Core Functionality

-Multi-source ETL ingestion

-Schema unification

-PostgreSQL persistence

-FastAPI data access

P1 — Extended Ingestion

-Third data source

-Robust data cleaning & normalization

P2 — Production Readiness

-API rate limiting (anti-spam protection)

-ETL run observability (etl_runs)

-Retry & resilience in API calls

-Containerized deployment

----------------------------------------------------------------------------------------------------
Running the Project

Prerequisites:

-Docker & Docker Compose

Start the System

docker compose up --build

API Endpoints

Get Data

curl http://localhost:8000/data

ETL Stats

curl http://localhost:8000/stats

Rate Limiting Test (Windows)

for /L %i in (1,1,70) do curl http://localhost:8000/data

(Returns 429 Too Many Requests after limit.)

----------------------------------------------------------------------------------------------------
Security & Configuration:

-No API keys hard-coded

-Environment variables via Docker

-Safe DB credentials handling

----------------------------------------------------------------------------------------------------
Author

Kritiraj Singh

CSE (IoT, Cybersecurity Including Blockchain Technology)
