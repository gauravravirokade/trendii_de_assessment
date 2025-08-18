# Installation & Run Guide

This guide explains how to set up, run, and access the **Event Analytics dbt project**.

---

## 🛠️ Prerequisites

1. **Python 3.8+**
   Recommended: use a virtual environment (e.g., `venv` or `conda`).

2. **Postgres database**
   Ensure you have access to your Postgres instance (local or Aurora).

3. **dbt Core**
   Install via pip:

   ```bash
   pip install dbt-postgres
   ```

---

## 📂 Project Setup

1. **Clone the repository**:

   ```bash
   git clone https://github.com/gauravravirokade/trendii_de_assessment.git
   cd trendii_de_assessment/analytics/event_analytics
   ```

2. **Set up a virtual environment** (recommended):

   ```bash
   python3 -m venv venv
   source venv/bin/activate   # on Mac/Linux
   venv\Scripts\activate      # on Windows
   ```

3. **Configure your profile** in `~/.dbt/profiles.yml`:

   ```yaml
   event_analytics:
     target: dev
     outputs:
       dev:
         dbname: trendii_de_assessment
         host: localhost
         pass: postgres
         port: 5432
         schema: grr_dev
         threads: 2
         type: postgres
         user: postgres

       prod:
         dbname: trendii_de_assessment
         host: localhost
         pass: postgres
         port: 5432
         schema: grr_dev
         threads: 2
         type: postgres
         user: postgres
   ```

> **Note:** Both dev and prod currently point to the same schema (`grr_dev`). You may choose to separate schemas to avoid overwriting data between environments.

---

## ▶️ Running dbt

### Default (Dev target)

Since `target: dev` is defined, dbt will run against **dev** unless otherwise specified.

* Run all models:

  ```bash
  dbt run
  ```

* Run tests:

  ```bash
  dbt test
  ```

* Run + test together:

  ```bash
  dbt build
  ```

* Run a specific model:

  ```bash
  dbt run -s stg_event_product_impressions
  ```

---

### Switching to Prod

Explicitly set the target to `prod` when executing:

* Run all models in **prod**:

  ```bash
  dbt run --target prod
  ```

* Run tests in **prod**:

  ```bash
  dbt test --target prod
  ```

* Build (run + test) in **prod**:

  ```bash
  dbt build --target prod
  ```

---

## 📖 Documentation

1. Generate dbt docs:

   ```bash
   dbt docs generate
   ```

2. Serve docs locally:

   ```bash
   dbt docs serve
   ```

3. **Host on GitHub Pages**:

   * Ensure `target/` is generated (`dbt docs generate`).
   * Push the contents of `target/` to the `gh-pages` branch.
   * Enable **Pages** in your repo settings pointing to `gh-pages` branch → `/ (root)`.
   * Access your docs at:

     ```
     https://<username>.github.io/<repository>/
     ```

---

## 🔧 Helpful Commands

* Debug profile connection:

  ```bash
  dbt debug
  ```

* Full refresh (Dev target):

  ```bash
  dbt run --full-refresh
  ```

* Full refresh (Prod target):

  ```bash
  dbt run --target prod --full-refresh
  ```
