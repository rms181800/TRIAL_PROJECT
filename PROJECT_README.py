# Databricks notebook source
# DBTITLE 1,PROJECT DOCUMENTATION
# MAGIC %md
# MAGIC # 🛒 RAHUL RETAIL STORE - END-TO-END DATA PLATFORM
# MAGIC ## 📊 E-Commerce Analytics & BI Dashboard Project
# MAGIC
# MAGIC ![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue) ![Status](https://img.shields.io/badge/Status-Production-success) ![Cloud](https://img.shields.io/badge/Cloud-Azure-0078D4) ![Engine](https://img.shields.io/badge/Engine-Spark-E25A1C)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 PROJECT OVERVIEW
# MAGIC
# MAGIC **👤 Created By**: Rahul M  
# MAGIC **📞 Contact**: 8551853004  
# MAGIC **📅 Created Date**: August 22, 2026  
# MAGIC **🏗️ Architecture**: Medallion (Bronze → Silver → Gold → Reporting)  
# MAGIC **☁️ Cloud Platform**: Azure Databricks  
# MAGIC **⚡ Engine**: Apache Spark 14.3.x (PySpark)  
# MAGIC **💾 Storage**: Delta Lake  
# MAGIC **🤖 Orchestration**: Databricks Workflow Job  
# MAGIC **📈 BI Dashboard**: Lakeview AI/BI Dashboard  
# MAGIC **🔄 Automation**: Daily at 2:00 AM UTC  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💼 BUSINESS PROBLEM
# MAGIC
# MAGIC Large e-commerce marketplace **"RAHUL RETAIL STORE"** requires a production-grade, automated data platform with:
# MAGIC
# MAGIC ✅ **Real-time Business Intelligence**
# MAGIC - 📊 Executive KPI Dashboard (Revenue, Orders, Sellers, Ratings)
# MAGIC - 📈 Daily revenue trends and growth tracking
# MAGIC - 🏆 Top seller and product performance
# MAGIC - 💳 Payment method analysis
# MAGIC - 📦 Category performance metrics
# MAGIC
# MAGIC ✅ **Automated Data Pipeline**
# MAGIC - 🔄 Automated ETL from landing → bronze → silver → gold
# MAGIC - 🤖 Job orchestration with dependency management
# MAGIC - ✅ Data quality validation
# MAGIC - 📧 Email alerts on failures
# MAGIC
# MAGIC ✅ **Advanced Analytics**
# MAGIC - 📍 Seller location history tracking (SCD-2)
# MAGIC - 🚚 Delivery performance metrics
# MAGIC - ⭐ Customer satisfaction analysis
# MAGIC - 📦 Product category insights
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ TECHNOLOGY STACK
# MAGIC
# MAGIC | Component | Technology | Purpose |
# MAGIC |-----------|-----------|----------|
# MAGIC | ☁️ **Cloud** | Azure Databricks | Data platform |
# MAGIC | ⚡ **Engine** | Apache Spark 14.3.x | Distributed processing |
# MAGIC | 💻 **Language** | PySpark | Data transformations |
# MAGIC | 💾 **Storage** | Delta Lake | ACID transactions |
# MAGIC | 📁 **Format** | CSV → Delta | Data format |
# MAGIC | 🤖 **Orchestration** | Databricks Job | Workflow automation |
# MAGIC | 📊 **BI Tool** | Lakeview Dashboard | Visualization |
# MAGIC | 🗂️ **Catalog** | Unity Catalog | Data governance |
# MAGIC | 🏗️ **Architecture** | Medallion | Bronze, Silver, Gold, Reporting |
# MAGIC | ⏰ **Schedule** | Cron | Daily 2:00 AM UTC |

# COMMAND ----------

# DBTITLE 1,📁 PROJECT STRUCTURE & JOB ORCHESTRATION
# MAGIC %md
# MAGIC ## 📁 PROJECT STRUCTURE
# MAGIC
# MAGIC ```
# MAGIC 🗂️ AZURE_B3_PROJECT_AUG/
# MAGIC │
# MAGIC ├── 🥉 BRONZE/                           # Layer 1: Raw Data Ingestion (9 notebooks)
# MAGIC │   ├── 👥 customers
# MAGIC │   ├── 📦 orders
# MAGIC │   ├── 📋 order_items
# MAGIC │   ├── 💳 order_payments
# MAGIC │   ├── ⭐ order_reviews
# MAGIC │   ├── 🛍️ products
# MAGIC │   ├── 🏪 sellers
# MAGIC │   ├── 🗺️ geolocation
# MAGIC │   └── 🌐 product_category_name_translation
# MAGIC │
# MAGIC ├── 🥈 SILVER/                           # Layer 2: Clean & Standardized (9 notebooks)
# MAGIC │   ├── 👥 customers_silver
# MAGIC │   ├── 📦 orders_silver
# MAGIC │   ├── 📋 order_items_silver
# MAGIC │   ├── 💳 order_payments_silver
# MAGIC │   ├── ⭐ order_reviews_silver
# MAGIC │   ├── 🛍️ products_silver
# MAGIC │   ├── 🏪 sellers_silver
# MAGIC │   ├── 🗺️ geolocation_silver
# MAGIC │   └── 🌐 category_translation_silver
# MAGIC │
# MAGIC ├── 🥇 GOLD/                             # Layer 3: Business-Ready Dimensions & Facts (4 notebooks)
# MAGIC │   ├── 👤 dim_customer           (SCD-1: Overwrite)
# MAGIC │   ├── 🛍️ dim_product            (SCD-1: Overwrite)
# MAGIC │   ├── 🏪 dim_seller             (SCD-2: History Tracking)
# MAGIC │   └── 📊 fact_orders_and_analytics
# MAGIC │
# MAGIC ├── 📈 REPORTING/                        # Layer 4: Materialized Views for Dashboard (3 notebooks)
# MAGIC │   ├── 💰 MV_DAILY_REVENUE              → Powers revenue KPIs & trend charts
# MAGIC │   ├── 🏆 MV_SELLER_PERFORMANCE         → Powers top sellers chart
# MAGIC │   └── 📦 MV_PRODUCT_PERFORMANCE        → Powers category analysis
# MAGIC │
# MAGIC ├── ⚙️ FUNCTIONS/                        # Reusable utility functions
# MAGIC │   └── 🔧 functions
# MAGIC │
# MAGIC ├── ✅ TESTING/                          # Test scripts
# MAGIC │
# MAGIC ├── 🤖 MASTER_ORCHESTRATION              # Legacy notebook-based orchestration
# MAGIC ├── ✅ DATA_QUALITY_CHECKS               # Validation scripts
# MAGIC ├── 📄 PROJECT_README                    # This file
# MAGIC └── 📋 RETAIL_PROJECT_FINAL_RAHUL_M.pdf  # Project documentation
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🤖 AUTOMATED JOB ORCHESTRATION
# MAGIC
# MAGIC ### 📊 Job Configuration
# MAGIC
# MAGIC **Job Name**: `AZURE_B3_RETAIL_DASHBOARD_REFRESH`  
# MAGIC **Job ID**: `456724186190637`  
# MAGIC **Schedule**: ⏰ Daily at **2:00 AM UTC**  
# MAGIC **Status**: 🟢 **Active & Running**  
# MAGIC **Total Tasks**: **25 notebooks** across 5 layers  
# MAGIC **Max Concurrency**: **9 parallel tasks**  
# MAGIC **Compute**: Serverless (auto-scaling)  
# MAGIC **Alerts**: 📧 rms181800@gmail.com on failure
# MAGIC
# MAGIC ### 🔄 Pipeline Execution Flow
# MAGIC
# MAGIC ```
# MAGIC 🔽 LAYER 1: BRONZE (9 tasks run in PARALLEL)
# MAGIC    │
# MAGIC    ├─ 🥉 bronze_customers
# MAGIC    ├─ 🥉 bronze_sellers
# MAGIC    ├─ 🥉 bronze_products
# MAGIC    ├─ 🥉 bronze_orders
# MAGIC    ├─ 🥉 bronze_order_items
# MAGIC    ├─ 🥉 bronze_order_payments
# MAGIC    ├─ 🥉 bronze_order_reviews
# MAGIC    ├─ 🥉 bronze_geolocation
# MAGIC    └─ 🥉 bronze_category_translation
# MAGIC           │
# MAGIC           ↓
# MAGIC 🔽 LAYER 2: SILVER (9 tasks run in PARALLEL, 1:1 dependencies)
# MAGIC    │
# MAGIC    ├─ 🥈 silver_customers       ← bronze_customers
# MAGIC    ├─ 🥈 silver_sellers         ← bronze_sellers
# MAGIC    ├─ 🥈 silver_products        ← bronze_products
# MAGIC    ├─ 🥈 silver_orders          ← bronze_orders
# MAGIC    ├─ 🥈 silver_order_items     ← bronze_order_items
# MAGIC    ├─ 🥈 silver_order_payments  ← bronze_order_payments
# MAGIC    ├─ 🥈 silver_order_reviews   ← bronze_order_reviews
# MAGIC    ├─ 🥈 silver_geolocation     ← bronze_geolocation
# MAGIC    └─ 🥈 silver_category_trans  ← bronze_category_translation
# MAGIC           │
# MAGIC           ↓
# MAGIC 🔽 LAYER 3: GOLD DIMENSIONS (3 tasks run in PARALLEL)
# MAGIC    │
# MAGIC    ├─ 🥇 gold_dim_customer  ← silver_customers
# MAGIC    ├─ 🥇 gold_dim_product   ← silver_products
# MAGIC    └─ 🥇 gold_dim_seller    ← silver_sellers
# MAGIC           │
# MAGIC           ↓
# MAGIC 🔽 LAYER 4: GOLD FACT (1 task)
# MAGIC    │
# MAGIC    └─ 🥇 gold_fact_orders   ← silver_orders + silver_order_items + 
# MAGIC                                 silver_order_payments + silver_order_reviews
# MAGIC           │
# MAGIC           ↓
# MAGIC 🔽 LAYER 5: REPORTING (3 Materialized Views run in PARALLEL)
# MAGIC    │
# MAGIC    ├─ 📈 MV_DAILY_REVENUE         ← gold_fact_orders
# MAGIC    ├─ 📈 MV_SELLER_PERFORMANCE    ← gold_fact_orders
# MAGIC    └─ 📈 MV_PRODUCT_PERFORMANCE   ← gold_fact_orders
# MAGIC           │
# MAGIC           ↓
# MAGIC 🎉 DASHBOARD AUTO-REFRESHES WITH FRESH DATA!
# MAGIC ```
# MAGIC
# MAGIC **🎯 Key Features:**
# MAGIC - ✅ Proper 1:1 dependencies (no unnecessary blocking)
# MAGIC - ✅ Maximum parallelization within each layer
# MAGIC - ✅ Auto-scaling serverless compute
# MAGIC - ✅ 60-minute timeout per task
# MAGIC - ✅ Automatic failure notifications
# MAGIC - ✅ Dashboard updates automatically after job completion

# COMMAND ----------

# DBTITLE 1,🏗️ COLORFUL ARCHITECTURE DIAGRAM
# MAGIC %md
# MAGIC ## 🏗️ END-TO-END ARCHITECTURE FLOW
# MAGIC
# MAGIC ```
# MAGIC 📂 DATA SOURCES (CSV Files in Landing Zone)
# MAGIC   │
# MAGIC   ├─ customers.csv
# MAGIC   ├─ sellers.csv  
# MAGIC   ├─ products.csv
# MAGIC   ├─ orders.csv
# MAGIC   ├─ order_items.csv
# MAGIC   ├─ order_payments.csv
# MAGIC   ├─ order_reviews.csv
# MAGIC   ├─ geolocation.csv
# MAGIC   └─ product_category_name_translation.csv
# MAGIC          │
# MAGIC          ↓ 📥 FULL LOAD (Auto Loader)
# MAGIC          │
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │  🥉 BRONZE LAYER - Raw Data (Delta Lake)               │
# MAGIC │  • No transformations, as-is data                        │
# MAGIC │  • Add ingest_ts for audit trail                        │  
# MAGIC │  • 9 Delta tables in retail_project_b3.bronze           │
# MAGIC │  • Stored in /Volumes/retail_project_b3/bronze/         │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC          │
# MAGIC          ↓ 🧙 CLEANSE & STANDARDIZE
# MAGIC          │
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │  🥈 SILVER LAYER - Clean & Standardized               │
# MAGIC │  • Remove nulls, deduplicate, lowercase/uppercase       │
# MAGIC │  • Handle missing values, validate data types          │
# MAGIC │  • 9 Delta tables in retail_project_b3.silver          │
# MAGIC │  • Stored in /Volumes/retail_project_b3/silver/        │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC          │
# MAGIC          ↓ ⭐ BUSINESS MODELING
# MAGIC          │
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │  🥇 GOLD LAYER - Star Schema (Dimensional Model)      │
# MAGIC │                                                        │
# MAGIC │  👤 dim_customer (SCD-1)                              │
# MAGIC │     - customer_id, city, state, zip                   │
# MAGIC │                                                        │
# MAGIC │  🛍️ dim_product (SCD-1)                               │
# MAGIC │     - product_id, category, dimensions, weight        │
# MAGIC │                                                        │
# MAGIC │  🏪 dim_seller (SCD-2 with history)                   │
# MAGIC │     - seller_id, city, state, effective_from/to       │
# MAGIC │                                                        │
# MAGIC │  📊 fact_orders (grain: order_id + order_item_id)    │
# MAGIC │     - FKs: customer_id, product_id, seller_id         │
# MAGIC │     - Measures: price, freight, payment, rating       │
# MAGIC │                                                        │
# MAGIC │  • 4 Delta tables in retail_project_b3.gold            │
# MAGIC │  • Stored in /Volumes/retail_project_b3/gold/          │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC          │
# MAGIC          ↓ 📈 AGGREGATE & MATERIALIZE
# MAGIC          │
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │  📈 REPORTING LAYER - Materialized Views               │
# MAGIC │                                                        │
# MAGIC │  💰 mv_daily_revenue                                  │
# MAGIC │     - Daily aggregated revenue, orders, customers     │
# MAGIC │     - Powers: KPI cards, trend charts                 │
# MAGIC │                                                        │
# MAGIC │  🏆 mv_seller_performance                             │
# MAGIC │     - Seller metrics: revenue, orders, ratings        │
# MAGIC │     - Powers: Top sellers chart                       │
# MAGIC │                                                        │
# MAGIC │  📦 mv_product_performance                            │
# MAGIC │     - Category revenue, order count, avg rating       │
# MAGIC │     - Powers: Category analysis charts & table        │
# MAGIC │                                                        │
# MAGIC │  • 3 Materialized Views in retail_project_b3.gold     │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC          │
# MAGIC          ↓ 🔌 QUERY & VISUALIZE
# MAGIC          │
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │  📊 LAKEVIEW BI DASHBOARD                             │
# MAGIC │  "Rahul Retail Store - Executive Dashboard"          │
# MAGIC │                                                        │
# MAGIC │  🟢 10 Colorful Widgets:                              │
# MAGIC │     • Total Revenue (KPI counter)                     │
# MAGIC │     • Total Orders (KPI counter)                      │
# MAGIC │     • Active Sellers (KPI counter)                    │
# MAGIC │     • Average Rating (KPI counter with color)         │
# MAGIC │     • Daily Revenue Trend (area chart)                │
# MAGIC │     • Orders by Day of Week (bar chart)               │
# MAGIC │     • Top 10 Sellers by City (grouped bar)            │
# MAGIC │     • Revenue by Payment Method (pie chart)           │
# MAGIC │     • Top 10 Product Categories (horizontal bar)      │
# MAGIC │     • Category Performance Table                      │
# MAGIC │                                                        │
# MAGIC │  ✅ Auto-refreshes daily after job completion!         │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,🗂️ UNITY CATALOG STRUCTURE
# MAGIC %md
# MAGIC ## 🗂️ UNITY CATALOG STRUCTURE
# MAGIC
# MAGIC **Catalog Name**: `retail_project_b3`
# MAGIC
# MAGIC ### 📂 Schemas & Tables
# MAGIC
# MAGIC ```
# MAGIC 🗂️ retail_project_b3 (Unity Catalog)
# MAGIC │
# MAGIC ├── 📍 landing/                    # Raw CSV files from source
# MAGIC │   ├── customers.csv
# MAGIC │   ├── sellers.csv
# MAGIC │   ├── products.csv
# MAGIC │   ├── orders.csv
# MAGIC │   ├── order_items.csv
# MAGIC │   ├── order_payments.csv
# MAGIC │   ├── order_reviews.csv
# MAGIC │   ├── geolocation.csv
# MAGIC │   └── product_category_name_translation.csv
# MAGIC │
# MAGIC ├── 🥉 bronze/                     # Raw Delta tables (9 tables)
# MAGIC │   ├── customer                 (Delta - /Volumes/retail_project_b3/bronze/customer/)
# MAGIC │   ├── seller                   (Delta - /Volumes/retail_project_b3/bronze/seller/)
# MAGIC │   ├── product                  (Delta - /Volumes/retail_project_b3/bronze/product/)
# MAGIC │   ├── orders                   (Delta - /Volumes/retail_project_b3/bronze/orders/)
# MAGIC │   ├── order_items              (Delta - /Volumes/retail_project_b3/bronze/order_items/)
# MAGIC │   ├── order_payments           (Delta - /Volumes/retail_project_b3/bronze/order_payments/)
# MAGIC │   ├── order_reviews            (Delta - /Volumes/retail_project_b3/bronze/order_reviews/)
# MAGIC │   ├── geolocation              (Delta - /Volumes/retail_project_b3/bronze/geolocation/)
# MAGIC │   └── category_translation     (Delta - /Volumes/retail_project_b3/bronze/category_translation/)
# MAGIC │
# MAGIC ├── 🥈 silver/                    # Clean Delta tables (9 tables)
# MAGIC │   ├── customers                (Table - retail_project_b3.silver.customers)
# MAGIC │   ├── sellers                  (Table - retail_project_b3.silver.sellers)
# MAGIC │   ├── products                 (Table - retail_project_b3.silver.products)
# MAGIC │   ├── orders                   (Table - retail_project_b3.silver.orders)
# MAGIC │   ├── order_items              (Table - retail_project_b3.silver.order_items)
# MAGIC │   ├── order_payments           (Table - retail_project_b3.silver.order_payments)
# MAGIC │   ├── order_reviews            (Table - retail_project_b3.silver.order_reviews)
# MAGIC │   ├── geolocation              (Table - retail_project_b3.silver.geolocation)
# MAGIC │   └── category_translation     (Table - retail_project_b3.silver.category_translation)
# MAGIC │
# MAGIC └── 🥇 gold/                      # Star schema + Materialized Views (7 tables)
# MAGIC     │
# MAGIC     ├── 👤 dim_customer            (SCD-1 - Overwrite)
# MAGIC     │   - customer_id (PK)
# MAGIC     │   - customer_city, customer_state, customer_zip_code_prefix
# MAGIC     │
# MAGIC     ├── 🛍️ dim_product             (SCD-1 - Overwrite)
# MAGIC     │   - product_id (PK)
# MAGIC     │   - product_category_name, category_english
# MAGIC     │   - product dimensions (length, height, width, weight)
# MAGIC     │
# MAGIC     ├── 🏪 dim_seller              (SCD-2 - History Tracking)
# MAGIC     │   - seller_key (Surrogate PK)
# MAGIC     │   - seller_id (Natural Key)
# MAGIC     │   - seller_city, seller_state, seller_zip_code_prefix
# MAGIC     │   - effective_from, effective_to, is_current
# MAGIC     │
# MAGIC     ├── 📊 fact_orders             (Fact Table)
# MAGIC     │   - order_id (PK)
# MAGIC     │   - order_item_id (PK)
# MAGIC     │   - customer_id (FK), product_id (FK), seller_id (FK)
# MAGIC     │   - date_id (for partitioning)
# MAGIC     │   - Measures: price, freight, payment_value, review_score, delivery_days
# MAGIC     │
# MAGIC     ├── 💰 mv_daily_revenue         (Materialized View)
# MAGIC     │   - order_date, daily_revenue, order_count
# MAGIC     │   - unique_customers, avg_order_value
# MAGIC     │   - payment_method columns (credit_card, boleto, voucher, debit_card)
# MAGIC     │
# MAGIC     ├── 🏆 mv_seller_performance   (Materialized View)
# MAGIC     │   - seller_id, seller_city, seller_state
# MAGIC     │   - total_revenue, total_orders, avg_delivery_days, avg_rating
# MAGIC     │
# MAGIC     └── 📦 mv_product_performance  (Materialized View)
# MAGIC         - category_english
# MAGIC         - total_revenue, order_count, avg_rating
# MAGIC ```
# MAGIC
# MAGIC ### 💾 Storage Locations
# MAGIC
# MAGIC | Layer | Unity Catalog Location | Volume Path |
# MAGIC |-------|------------------------|-------------|
# MAGIC | 🥉 Bronze | `retail_project_b3.bronze.*` | `/Volumes/retail_project_b3/bronze/*/` |
# MAGIC | 🥈 Silver | `retail_project_b3.silver.*` | `/Volumes/retail_project_b3/silver/*/` |
# MAGIC | 🥇 Gold | `retail_project_b3.gold.*` | `/Volumes/retail_project_b3/gold/*/` |
# MAGIC | 📊 Reporting | `retail_project_b3.gold.mv_*` | Materialized Views (no separate storage) |

# COMMAND ----------

# DBTITLE 1,🎨 COLORFUL EXECUTIVE DASHBOARD
# MAGIC %md
# MAGIC ## 🎨 RAHUL RETAIL STORE - EXECUTIVE DASHBOARD
# MAGIC
# MAGIC **Dashboard Name**: "Rahul Retail Store - Executive Dashboard 2026-08-22T17-36-38"  
# MAGIC **Type**: Lakeview AI/BI Dashboard  
# MAGIC **Status**: 🟢 **Production-Ready**  
# MAGIC **Auto-Refresh**: ✅ **Daily at 2:00 AM UTC** (via job)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎭 Dashboard Layout & Widgets
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                  🏆 RAHUL RETAIL STORE - EXECUTIVE DASHBOARD 🏆                     │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                              │
# MAGIC │  ROW 1: KPI CARDS                                                             │
# MAGIC │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
# MAGIC │  │ 💰 TOTAL REVENUE │  │ 📦 TOTAL ORDERS │  │ 🏪 ACTIVE SELLERS│  │ ⭐ AVG RATING    │  │
# MAGIC │  │                 │  │                 │  │                 │  │                 │  │
# MAGIC │  │  $16.8M         │  │   112,650       │  │     3,095       │  │   🟢 4.1       │  │
# MAGIC │  │  ↑ +15.3%      │  │  ↑ +8.2%       │  │                 │  │                 │  │
# MAGIC │  └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
# MAGIC │                                                                              │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                              │
# MAGIC │  ROW 2: TREND CHARTS                                                          │
# MAGIC │  ┌───────────────────────────────────┐  ┌───────────────────────────────────┐  │
# MAGIC │  │ 📈 Daily Revenue Trend (90 days) │  │ 📏 Orders by Day of Week      │  │
# MAGIC │  │                                   │  │                                   │  │
# MAGIC │  │       /\      /\                  │  │  Mon ███████████████       │  │
# MAGIC │  │      /  \    /  \                 │  │  Tue ████████████████      │  │
# MAGIC │  │     /    \  /    \                │  │  Wed █████████████████     │  │
# MAGIC │  │ ___/      \/      \___            │  │  Thu ██████████████         │  │
# MAGIC │  │                                   │  │  Fri ██████████████████    │  │
# MAGIC │  └───────────────────────────────────┘  └───────────────────────────────────┘  │
# MAGIC │                                                                              │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                              │
# MAGIC │  ROW 3: PERFORMANCE ANALYSIS                                                  │
# MAGIC │  ┌───────────────────────────────────┐  ┌───────────────────────────────────┐  │
# MAGIC │  │ 🏆 Top 10 Sellers by City      │  │ 💳 Revenue by Payment Method  │  │
# MAGIC │  │                                   │  │                                   │  │
# MAGIC │  │  Sao Paulo ██████████████    │  │      ___                        │  │
# MAGIC │  │  Rio       ████████████      │  │     /   \   Credit 65%          │  │
# MAGIC │  │  Curitiba  ██████████        │  │    |     |  Boleto 23%          │  │
# MAGIC │  │  Belo Horiz████████          │  │    |_____|  Voucher 8%          │  │
# MAGIC │  │  ...                               │  │             Debit 4%            │  │
# MAGIC │  └───────────────────────────────────┘  └───────────────────────────────────┘  │
# MAGIC │                                                                              │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                              │
# MAGIC │  ROW 4: CATEGORY ANALYSIS                                                     │
# MAGIC │  ┌───────────────────────────────────┐  ┌───────────────────────────────────┐  │
# MAGIC │  │ 📦 Top 10 Product Categories    │  │ 📊 Category Performance Table │  │
# MAGIC │  │                                   │  │                                   │  │
# MAGIC │  │  Health Beauty ████████████  │  │  Category   | Revenue | Orders  │  │
# MAGIC │  │  Watches       ██████████    │  │  -----------|---------|-------  │  │
# MAGIC │  │  Bed Bath      █████████     │  │  Health B.. | $1.2M   | 13.5K   │  │
# MAGIC │  │  Sports        ████████       │  │  Watches    | $850K   | 9.2K    │  │
# MAGIC │  │  ...                               │  │  ...                               │  │
# MAGIC │  └───────────────────────────────────┘  └───────────────────────────────────┘  │
# MAGIC │                                                                              │
# MAGIC └──────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📈 Dashboard Widgets Details
# MAGIC
# MAGIC | Widget # | Title | Type | Data Source | Description |
# MAGIC |----------|-------|------|-------------|-------------|
# MAGIC | 1 | 💰 Total Revenue | Counter | `mv_daily_revenue` | Sum of daily_revenue with growth % |
# MAGIC | 2 | 📦 Total Orders | Counter | `mv_daily_revenue` | Sum of order_count with growth % |
# MAGIC | 3 | 🏪 Active Sellers | Counter | `mv_seller_performance` | Count of distinct sellers |
# MAGIC | 4 | ⭐ Average Rating | Counter | `mv_seller_performance` | Avg of avg_rating (color-coded: 🟢>=4, 🟡 3-4, 🔴<3) |
# MAGIC | 5 | 📈 Daily Revenue Trend | Area Chart | `mv_daily_revenue` | order_date (X) vs daily_revenue (Y), last 90 days |
# MAGIC | 6 | 📏 Orders by Day of Week | Bar Chart | `mv_daily_revenue` | Horizontal bars, sorted by weekday |
# MAGIC | 7 | 🏆 Top 10 Sellers | Grouped Bar | `mv_seller_performance` | Group by seller_city, sum(total_revenue), top 10 |
# MAGIC | 8 | 💳 Revenue by Payment | Pie Chart | `datasets/payment_method` | Payment method split (credit, boleto, voucher, debit) |
# MAGIC | 9 | 📦 Top 10 Categories | Horizontal Bar | `mv_product_performance` | category_english (Y) vs total_revenue (X), top 10 |
# MAGIC | 10 | 📊 Category Performance | Table | `mv_product_performance` | category, revenue, orders, avg_rating |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 How Dashboard Updates Automatically
# MAGIC
# MAGIC 1. **Daily at 2:00 AM UTC** → Job `AZURE_B3_RETAIL_DASHBOARD_REFRESH` starts
# MAGIC 2. **BRONZE layer** → 9 notebooks ingest fresh CSV data from landing zone
# MAGIC 3. **SILVER layer** → 9 notebooks cleanse and standardize data
# MAGIC 4. **GOLD layer** → 4 notebooks build star schema (dims + fact)
# MAGIC 5. **REPORTING layer** → 3 notebooks REFRESH materialized views
# MAGIC 6. **Dashboard** → Automatically displays fresh data (no manual refresh needed!)
# MAGIC
# MAGIC ✅ **Users see updated metrics when they open the dashboard!**

# COMMAND ----------

# DBTITLE 1,⚙️ TRANSFORMATION LOGIC & DATA QUALITY
# MAGIC %md
# MAGIC ## ⚙️ TRANSFORMATION LOGIC BY LAYER
# MAGIC
# MAGIC ### 🥉 BRONZE LAYER
# MAGIC - ✅ Store raw data exactly as received (no transformations)
# MAGIC - ✅ Add `ingest_ts` audit timestamp  
# MAGIC - ✅ Write in Delta format for ACID compliance
# MAGIC - ✅ No deduplication or data quality filtering
# MAGIC - ✅ Full load from CSV files in landing zone
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🥈 SILVER LAYER TRANSFORMATIONS
# MAGIC
# MAGIC #### 👥 Customers
# MAGIC - 🔡 Lowercase city names for standardization
# MAGIC - 🔠 Capitalize state codes (e.g., sp → SP)
# MAGIC - 🧹 Remove spaces from ZIP codes
# MAGIC - ♻️ Deduplicate by `customer_id` (latest record wins)
# MAGIC - ❌ Filter null `customer_id`
# MAGIC
# MAGIC #### 📦 Orders
# MAGIC - 📅 Cast all timestamp columns to proper datetime
# MAGIC - 📊 Calculate `delivery_days` = delivered_date - purchase_date
# MAGIC - ❌ Filter null `order_id` or `customer_id`
# MAGIC - ♻️ Deduplicate by `order_id`
# MAGIC
# MAGIC #### 📋 Order Items
# MAGIC - 💲 Remove $ symbols from `price` and `freight_value`
# MAGIC - ✅ Validate non-negative numeric values
# MAGIC - 🔢 Cast to `double` data type
# MAGIC - ❌ Filter null `order_id` or `product_id`
# MAGIC
# MAGIC #### 💳 Order Payments
# MAGIC - 🧹 Remove special characters from `payment_type`
# MAGIC - 📦 Aggregate by `order_id` (multiple payments per order)
# MAGIC - 📊 Sum `payment_installments` and `payment_value`
# MAGIC
# MAGIC #### ⭐ Order Reviews
# MAGIC - 📊 Default `review_score` to 0 if null
# MAGIC - ✅ Validate score range (1-5)
# MAGIC - 📝 Handle null review comments
# MAGIC - ♻️ Deduplicate by `order_id` (one review per order)
# MAGIC
# MAGIC #### 🛍️ Products
# MAGIC - 🌐 Join with category translation for English names
# MAGIC - 📊 Cast dimension columns (weight, length, height, width) to numeric
# MAGIC - ♻️ Deduplicate by `product_id`
# MAGIC - ❌ Filter null `product_id`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🥇 GOLD LAYER MODELING
# MAGIC
# MAGIC #### 👤 dim_customer (SCD-1)
# MAGIC - 🔑 Primary Key: `customer_id`
# MAGIC - 📊 Attributes: city, state, zip_code
# MAGIC - 🔄 SCD-1: Overwrites on change (no history)
# MAGIC
# MAGIC #### 🛍️ dim_product (SCD-1)
# MAGIC - 🔑 Primary Key: `product_id`
# MAGIC - 📊 Attributes: category (Portuguese + English), dimensions, weight, photos
# MAGIC - 🔄 SCD-1: Overwrites on change
# MAGIC
# MAGIC #### 🏪 dim_seller (SCD-2)
# MAGIC - 🔑 Surrogate Key: `seller_key` (auto-generated)
# MAGIC - 🔑 Natural Key: `seller_id`
# MAGIC - 📊 Attributes: city, state, zip_code
# MAGIC - 📅 History: `effective_from`, `effective_to`, `is_current` flag
# MAGIC - 🔄 SCD-2: Tracks location changes over time (new row per change)
# MAGIC
# MAGIC #### 📊 fact_orders
# MAGIC - 🔑 Composite Key: `order_id` + `order_item_id`
# MAGIC - 🔗 Foreign Keys: `customer_id`, `product_id`, `seller_id`, `date_id`
# MAGIC - 📊 Measures: `price`, `freight_value`, `payment_value`, `review_score`, `delivery_days`, `total_revenue`
# MAGIC - 📆 Partitioned by: `date_id` (YYYYMMDD format)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📈 REPORTING LAYER - Materialized Views
# MAGIC
# MAGIC #### 💰 mv_daily_revenue
# MAGIC - 📅 Grain: One row per order_date
# MAGIC - 📊 Aggregations: daily_revenue, order_count, unique_customers, avg_order_value
# MAGIC - 💳 Payment splits: credit_card, boleto, voucher, debit_card
# MAGIC - 🎯 Powers: KPI cards, revenue trend chart, payment pie chart
# MAGIC
# MAGIC #### 🏆 mv_seller_performance
# MAGIC - 🏪 Grain: One row per seller_id + seller_city
# MAGIC - 📊 Aggregations: total_revenue, total_orders, avg_delivery_days, avg_rating
# MAGIC - 🗺️ Joins: dim_seller (current records only)
# MAGIC - 🎯 Powers: Top 10 sellers chart
# MAGIC
# MAGIC #### 📦 mv_product_performance
# MAGIC - 📦 Grain: One row per category_english
# MAGIC - 📊 Aggregations: total_revenue, order_count, avg_rating
# MAGIC - 🛍️ Joins: dim_product
# MAGIC - 🎯 Powers: Top 10 categories chart, category performance table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ DATA QUALITY CHECKS
# MAGIC
# MAGIC The `DATA_QUALITY_CHECKS` notebook validates:
# MAGIC
# MAGIC 1. **📊 Row Count Validation**
# MAGIC    - All tables have data (count > 0)
# MAGIC    - Compare row counts across layers
# MAGIC
# MAGIC 2. **🔑 Primary Key Null Checks**
# MAGIC    - No null values in PK columns
# MAGIC    - Ensures data integrity
# MAGIC
# MAGIC 3. **♻️ Duplicate Detection**
# MAGIC    - Check for duplicate records in dimensions
# MAGIC    - Validates deduplication logic
# MAGIC
# MAGIC 4. **🔗 Foreign Key Integrity**
# MAGIC    - Validates FK relationships between fact and dimensions
# MAGIC    - Identifies orphan records
# MAGIC
# MAGIC 5. **💼 Business Rule Validation**
# MAGIC    - Price/freight are non-negative
# MAGIC    - Review scores are in range 0-5
# MAGIC    - Delivery days are positive
# MAGIC    - Payment values match order totals

# COMMAND ----------

# DBTITLE 1,🎉 PROJECT SUMMARY & HOW TO RUN
# MAGIC %md
# MAGIC ## 🎉 PROJECT DELIVERABLES SUMMARY
# MAGIC
# MAGIC ### ✅ Completed Components
# MAGIC
# MAGIC | Component | Count | Status |
# MAGIC |-----------|-------|--------|
# MAGIC | 🥉 Bronze Notebooks | 9 | 🟢 Complete |
# MAGIC | 🥈 Silver Notebooks | 9 | 🟢 Complete |
# MAGIC | 🥇 Gold Notebooks | 4 | 🟢 Complete |
# MAGIC | 📈 Reporting MVs | 3 | 🟢 Complete |
# MAGIC | 🤖 Orchestration Job | 1 (25 tasks) | 🟢 Active |
# MAGIC | 🎨 Executive Dashboard | 1 (10 widgets) | 🟢 Production |
# MAGIC | ✅ Data Quality Checks | 1 notebook | 🟢 Complete |
# MAGIC | 📄 Documentation | This README | 🟢 Complete |
# MAGIC
# MAGIC **Total Notebooks**: 26  
# MAGIC **Total Tables/Views**: 21 (9 bronze + 9 silver + 4 gold + 3 MVs)  
# MAGIC **Total Job Tasks**: 25  
# MAGIC **Dashboard Widgets**: 10
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 HOW TO RUN THE PROJECT
# MAGIC
# MAGIC ### 🎯 Option 1: Automated (Recommended)
# MAGIC
# MAGIC **The job runs automatically daily at 2:00 AM UTC!**
# MAGIC
# MAGIC To manually trigger:
# MAGIC ```bash
# MAGIC databricks jobs run-now 456724186190637
# MAGIC ```
# MAGIC
# MAGIC Or from Databricks UI:
# MAGIC 1. Navigate to **Workflows** → **Jobs**
# MAGIC 2. Find job: `AZURE_B3_RETAIL_DASHBOARD_REFRESH`
# MAGIC 3. Click **"Run now"**
# MAGIC
# MAGIC 👉 The entire pipeline executes automatically (Bronze → Silver → Gold → Reporting)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💻 Option 2: Manual Execution
# MAGIC
# MAGIC #### Run Master Orchestration Notebook
# MAGIC ```python
# MAGIC dbutils.notebook.run(
# MAGIC     "/Workspace/Users/rms181800@gmail.com/AZURE_B3_PROJECT_AUG/MASTER_ORCHESTRATION",
# MAGIC     timeout_seconds=3600
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC This executes:
# MAGIC - All 9 Silver transformation notebooks
# MAGIC - All 3 Gold dimension notebooks  
# MAGIC - Fact table and analytics notebook
# MAGIC - Error handling and logging
# MAGIC
# MAGIC #### Run Individual Layer
# MAGIC
# MAGIC **Bronze Layer:**
# MAGIC ```python
# MAGIC dbutils.notebook.run(".../BRONZE/customers", 600)
# MAGIC dbutils.notebook.run(".../BRONZE/orders", 600)
# MAGIC # ... repeat for all 9 bronze notebooks
# MAGIC ```
# MAGIC
# MAGIC **Silver Layer:**
# MAGIC ```python
# MAGIC dbutils.notebook.run(".../SILVER/customers_silver", 600)
# MAGIC dbutils.notebook.run(".../SILVER/orders_silver", 600)
# MAGIC # ... repeat for all 9 silver notebooks
# MAGIC ```
# MAGIC
# MAGIC **Gold Layer:**
# MAGIC ```python
# MAGIC dbutils.notebook.run(".../GOLD/dim_customer", 600)
# MAGIC dbutils.notebook.run(".../GOLD/dim_product", 600)
# MAGIC dbutils.notebook.run(".../GOLD/dim_seller", 600)
# MAGIC dbutils.notebook.run(".../GOLD/fact_orders_and_analytics", 1200)
# MAGIC ```
# MAGIC
# MAGIC **Reporting Layer:**
# MAGIC ```python
# MAGIC dbutils.notebook.run(".../REPORTING/MV_DAILY_REVENUE", 600)
# MAGIC dbutils.notebook.run(".../REPORTING/MV_SELLER_PERFORMANCE", 600)
# MAGIC dbutils.notebook.run(".../REPORTING/MV_PRODUCT_PERFORMANCE", 600)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 View the Dashboard
# MAGIC
# MAGIC 1. Navigate to **Dashboards** in Databricks
# MAGIC 2. Search for: **"Rahul Retail Store - Executive Dashboard"**
# MAGIC 3. Open the dashboard
# MAGIC 4. 🎉 View live metrics automatically refreshed from materialized views!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📈 SAMPLE BUSINESS QUERIES
# MAGIC
# MAGIC ### Query 1: Daily Revenue Trend
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     order_date,
# MAGIC     daily_revenue,
# MAGIC     order_count,
# MAGIC     unique_customers,
# MAGIC     avg_order_value
# MAGIC FROM retail_project_b3.gold.mv_daily_revenue
# MAGIC ORDER BY order_date DESC
# MAGIC LIMIT 30;
# MAGIC ```
# MAGIC
# MAGIC ### Query 2: Top 10 Sellers
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     seller_id,
# MAGIC     seller_city,
# MAGIC     total_revenue,
# MAGIC     total_orders,
# MAGIC     avg_delivery_days,
# MAGIC     avg_rating
# MAGIC FROM retail_project_b3.gold.mv_seller_performance
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC ### Query 3: Category Performance
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     category_english,
# MAGIC     total_revenue,
# MAGIC     order_count,
# MAGIC     avg_rating,
# MAGIC     ROUND(total_revenue / order_count, 2) AS avg_order_value
# MAGIC FROM retail_project_b3.gold.mv_product_performance
# MAGIC ORDER BY total_revenue DESC;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📧 CONTACT & SUPPORT
# MAGIC
# MAGIC **👤 Project Owner**: Rahul M  
# MAGIC **📞 Phone**: 8551853004  
# MAGIC **💼 Role**: Data Engineer  
# MAGIC **📍 Location**: India
# MAGIC
# MAGIC **For Questions:**
# MAGIC - 📧 Contact via phone: 8551853004
# MAGIC - 📝 Project documentation in this README
# MAGIC - 📊 Dashboard: "Rahul Retail Store - Executive Dashboard"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎆 PROJECT HIGHLIGHTS
# MAGIC
# MAGIC ✅ **Production-Ready Data Platform**
# MAGIC - ⚡ Fully automated ETL pipeline
# MAGIC - 🔄 Daily refresh at 2:00 AM UTC
# MAGIC - 📊 25 orchestrated tasks
# MAGIC - ✅ Data quality validation
# MAGIC - 📧 Email alerts on failure
# MAGIC
# MAGIC ✅ **Modern Architecture**
# MAGIC - 🏗️ Medallion (Bronze → Silver → Gold → Reporting)
# MAGIC - 💾 Delta Lake for ACID transactions
# MAGIC - 🗂️ Unity Catalog for governance
# MAGIC - 📊 Materialized views for performance
# MAGIC - 🥇 Star schema with SCD-2 for history
# MAGIC
# MAGIC ✅ **Executive BI Dashboard**
# MAGIC - 🎨 10 colorful widgets
# MAGIC - 📈 Real-time KPIs
# MAGIC - 🔄 Auto-refreshes daily
# MAGIC - 📊 Revenue, sellers, products, categories
# MAGIC - 🎯 Production-ready Lakeview dashboard
# MAGIC
# MAGIC ✅ **Comprehensive Coverage**
# MAGIC - 9 source entities
# MAGIC - 21 tables and materialized views
# MAGIC - 15+ business scenarios
# MAGIC - Complete data quality checks
# MAGIC - Full documentation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎉 **End of Documentation** 🎉
# MAGIC
# MAGIC **Thank you for reviewing the Rahul Retail Store Data Platform!**
# MAGIC
# MAGIC **🚀 The pipeline is live and running in production! 🚀**

# COMMAND ----------

# DBTITLE 1,DATA QUALITY CHECKS
# MAGIC %md
# MAGIC ## DATA QUALITY VALIDATION
# MAGIC
# MAGIC The `DATA_QUALITY_CHECKS` notebook performs:
# MAGIC
# MAGIC ### 1. Row Count Validation
# MAGIC - Ensures all tables have data (count > 0)
# MAGIC - Compares counts across layers
# MAGIC
# MAGIC ### 2. Primary Key Null Checks
# MAGIC - Validates no null values in PK columns
# MAGIC - Ensures data integrity
# MAGIC
# MAGIC ### 3. Duplicate Detection
# MAGIC - Checks for duplicate records in dimensions
# MAGIC - Validates deduplication logic
# MAGIC
# MAGIC ### 4. Foreign Key Integrity
# MAGIC - Validates FK relationships between fact and dimensions
# MAGIC - Identifies orphan records
# MAGIC
# MAGIC ### 5. Business Rule Validation
# MAGIC - Price is non-negative
# MAGIC - Review scores are 0-5
# MAGIC - Delivery days are positive for delivered orders
# MAGIC - Revenue calculation accuracy
# MAGIC
# MAGIC ### Quality Metrics
# MAGIC - Pass Rate calculation
# MAGIC - Detailed failure reporting
# MAGIC - Automated alerts on quality issues