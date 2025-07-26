# E-commerce Data Pipeline using Airflow, BigQuery & Dataproc

## Objective

Build a scalable, serverless data pipeline that:

- Ingests raw product and order data from Google Cloud Storage (GCS),
- Loads it into BigQuery staging tables,
- Transforms and joins the data using PySpark on Dataproc Serverless,
- Appends enriched records to a final BigQuery table:  
  `retail_data.enriched_orders`.

---

## Tools & Technologies

| Tool                        | Purpose                              |
|-----------------------------|--------------------------------------|
| **Apache Airflow**         | Orchestrate and schedule workflows   |
| **Astro CLI**              | Local development for Airflow DAGs   |
| **Google Cloud Storage**   | Store input JSON and PySpark scripts |
| **BigQuery**               | Store raw and transformed data       |
| **Dataproc Serverless**    | Run PySpark jobs without clusters    |
| **PySpark**                | Transform and join data              |

---

## Architecture
       +----------------------------+
       |     products.json          |
       |     orders.json            |
       +-------------+--------------+
                     |
                     v
     +-----------------------------+
     |  Load to BigQuery (Staging) |
     +-------------+---------------+
                   |
                   v
    +------------------------------+
    | Dataproc PySpark Job         |
    | (Transform & Join Logic)     |
    +-------------+----------------+
                  |
                  v
    +------------------------------+
    | Append to enriched_orders    |
    | table in BigQuery            |
    +------------------------------+


---

## 📁 Project Structure

```bash
ecomm-data-pipeline/
├── dags/
│   └── ecomm_data_pipeline_airflow_dag.py
├── scripts/
│   └── transform_join_ecommerce.py
├── datasets/
│   ├── products.json
│   └── orders.json
├── sql/
│   └── bigquery_commands.sql
└── README.md


## How to Run This Project

1. **Start Astro locally**  
   Open your terminal and run:
   ```bash
   astro dev start
   ```

2. **Open Airflow UI**  
   Navigate to [http://localhost:8080](http://localhost:8080) in your browser.

3. **Trigger DAG manually**  
   In the Airflow UI, trigger the DAG named:
   ```
   gcs_to_bq_dataproc_ecommerce
   ```

4. **Watch the pipeline execute:**
   - `products.json` → loaded into `retail_data.products`
   - `orders.json` → loaded into `retail_data.orders`
   - PySpark job (via Dataproc Serverless) joins both and appends to `retail_data.enriched_orders`

---

## Output

After a successful DAG run:

- products.json` → loaded into BigQuery table: `retail_data.products`
- orders.json` → loaded into BigQuery table: `retail_data.orders`
- Enriched result → appended to BigQuery table: `retail_data.enriched_orders`

--

## Key Learnings

- Built and orchestrated a real-time data pipeline using **Apache Airflow**
- Automated **GCS to BigQuery** ingestion with schema autodetection
- Leveraged **Dataproc Serverless** to transform and enrich data using **PySpark**
- Deployed a **scalable, serverless, cloud-native** solution end-to-end

---
