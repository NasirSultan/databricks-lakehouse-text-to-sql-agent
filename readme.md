# LakeMind

**A governed Databricks lakehouse with a self-checking Text-to-SQL analytics agent.**

Ask a business question in plain English. LakeMind turns it into SQL, checks that the SQL is safe, runs it on a governed Gold-layer view, and answers in a sentence. Every step is traced and the agent is scored on a test suite with MLflow.

> Personal project built on the public Olist Brazilian E-Commerce dataset from Kaggle. The data is anonymized and is not included in this repository.

---

## Demo video

**[Watch the full demo video](PASTE_YOUR_VIDEO_LINK_HERE)**

The video shows, in order:

1. The raw files and the Bronze, Silver and Gold tables in Unity Catalog
2. The lineage graph and the Job running the pipeline
3. The Text-to-SQL app answering a question, with the generated SQL expanded
4. A question the agent refuses and a dangerous request it blocks
5. The MLflow trace of one question and the evaluation results

---

## Table of contents

1. What this project does
2. Architecture
3. Tech stack
4. Repository structure
5. Dataset
6. How to run it, start to end
7. Data platform walkthrough
8. Text-to-SQL: full explanation
9. MLflow tracing and evaluation
10. Deploying the agent as a Databricks App
11. Problems found and how they were fixed
12. Results
13. Limitations
14. Future work

---

## 1. What this project does

**The problem.** Raw e-commerce data arrives as several CSV files that are duplicated, badly typed and spread across tables. Nobody can fully trust the numbers, and only people who know SQL can answer simple questions such as "Which state made the most revenue in 2018?".

**The solution.** Two connected parts:

| Part | What it delivers |
|---|---|
| Data platform | A Bronze, Silver and Gold lakehouse on Databricks with a star schema, a semantic layer, Unity Catalog governance and a scheduled Job |
| AI agent | A Text-to-SQL agent that reads only the governed Gold view, refuses questions the data cannot answer, blocks unsafe SQL, and is traced and tested with MLflow |

---

## 2. Architecture

### Data flow diagram

```
 Kaggle CSV files (5 files)
        |
        |  upload
        v
 Unity Catalog Volume   /Volumes/retail/bronze/raw_files
        |
        v
 +-----------+      +-----------+      +------------------------------+
 |  BRONZE   | ---> |  SILVER   | ---> |  GOLD                        |
 |  raw data |      |  cleaned  |      |  star schema                 |
 |  Delta    |      |  typed    |      |  fact_sales + 3 dimensions   |
 +-----------+      |  deduped  |      +--------------+---------------+
                    +-----------+                     |
                                                      v
                                       +------------------------------+
                                       |  SEMANTIC LAYER              |
                                       |  v_sales_metrics (view)      |
                                       |  one definition per metric   |
                                       +--------------+---------------+
                                                      |
                                                      v
                                       +------------------------------+
                                       |  TEXT-TO-SQL AGENT           |
                                       |  question -> SQL -> check -> |
                                       |  run -> answer               |
                                       +------+----------------+------+
                                              |                |
                                              v                v
                                      MLflow tracing    MLflow evaluation
                                              |
                                              v
                                    Databricks App (chat page)

 Orchestration : Databricks Job  (bronze -> silver -> gold -> quality checks)
 Governance    : Unity Catalog   (comments, PII tag, grants, lineage)
```

### Stage by stage

The data flows through these stages, in the order listed:

| Stage | What happens |
|---|---|
| 1. Kaggle CSV files | Five raw files are uploaded to a Unity Catalog volume |
| 2. Bronze | Raw copy stored as Delta tables with ingestion metadata |
| 3. Silver | Cleaned, deduplicated and correctly typed tables |
| 4. Gold | Star schema (one fact table, three dimensions) |
| 5. Semantic view | One governed view that defines the business metrics |
| 6. Text-to-SQL agent | Question in, SQL generated, checked, run, answer out |
| 7. Databricks App | A chat page where users ask questions |

Running alongside the flow:

| Concern | Tool |
|---|---|
| Orchestration | A Databricks Job runs bronze, then silver, then gold, then quality checks |
| Governance | Unity Catalog: comments, PII tag, grants and automatic lineage |
| Observability | MLflow tracing of every agent step |
| Quality | MLflow evaluation on a 20-question suite |

---

## 3. Tech stack

| Area | Tools |
|---|---|
| Platform | Databricks (Free Edition, serverless compute) |
| Processing | PySpark and Spark SQL |
| Storage | Delta Lake and Unity Catalog volumes |
| Governance | Unity Catalog (three-level namespace, tags, grants, lineage) |
| Modeling | Medallion architecture, star schema, semantic view |
| Orchestration | Databricks Jobs |
| LLM | Databricks-hosted Llama 3.3 70B Instruct |
| Observability | MLflow tracing and GenAI evaluation |
| App | Databricks Apps with Streamlit |

---

## 4. Repository structure

| Path | Contents |
|---|---|
| README.md | This file |
| .gitignore | Keeps data files and secrets out of the repo |
| notebooks/01_bronze | Raw ingestion |
| notebooks/02_silver | Cleaning and typing |
| notebooks/03_gold | Star schema and semantic view |
| notebooks/04_quality_checks | Data quality assertions |
| notebooks/10_sql_agent | Agent, tracing and evaluation |
| app/app.py | Streamlit chat app |
| app/app.yaml | Databricks App configuration |
| app/requirements.txt | App dependencies |

---

## 5. Dataset

Olist Brazilian E-Commerce (public and anonymized). Five files are used:

| File | Used for |
|---|---|
| Orders | Order status and timestamps |
| Order items | Items, price and freight per order |
| Customers | Customer city and state |
| Products | Product category |
| Order payments | Payment totals |

Download them from Kaggle. They are excluded from this repository.

The dataset has no customer names. One column identifies a real person across orders, while the per-order customer id changes with every order, so per-user totals must group by the person-level id.

---

## 6. How to run it, start to end

**Prerequisites:** a Databricks workspace (Free Edition works) and the five Olist CSV files.

1. **Create the structure.** Create a catalog named retail with three schemas (bronze, silver, gold) and a volume for the raw files.
2. **Upload the CSV files** to the volume through the Catalog page.
3. **Import the notebooks** from the notebooks folder into your workspace.
4. **Run in order:** bronze, silver, gold, then quality checks.
5. **Create the Job** with the four tasks chained in that order.
6. **Run the agent notebook** to test the agent and run the evaluation.
7. **Deploy the app** by following section 10.

If Free Edition does not allow a new catalog, use your default catalog and use its name wherever the notebooks say retail.

---

## 7. Data platform walkthrough

### 7.1 Unity Catalog structure

Unity Catalog uses a three-level namespace: catalog, schema, table. This project uses one catalog (retail) with a schema per layer (bronze, silver, gold) so raw, clean and final data never mix.

### 7.2 Bronze layer: raw ingestion

Rule: keep the data exactly as received and add metadata. No cleaning.

- Each CSV is read with its header and an inferred schema
- Two metadata columns are added: ingestion time and source file name
- Each file is saved as a Delta table in the bronze schema

Output: five Delta tables (orders, order items, customers, products, payments).

### 7.3 Silver layer: cleaning with PySpark

Rule: cleaned, typed and deduplicated.

| Step | What it does |
|---|---|
| Deduplication | One row per order, customer or product |
| Date conversion | Text timestamps become real timestamps and dates |
| Money casting | Price and freight become decimal columns |
| Missing values | Missing product category becomes "unknown" |
| Derived columns | Order date, delivery days, line total |
| Aggregation | Payments rolled up to one row per order |

### 7.4 SQL analysis

Joins, aggregations and window functions were practiced on the Silver tables:

| Technique | Example question |
|---|---|
| Multi-table join | Revenue by customer state |
| Ranking window function | Top products within each category |
| Running total | Cumulative daily revenue |
| Lag window function | Month-over-month revenue growth |

### 7.5 Delta Lake features

| Feature | What it gives you |
|---|---|
| ACID writes | A write fully succeeds or does not happen |
| Table history | A versioned log of every change |
| Time travel | Query the table as it was at an earlier version |
| Restore | Undo a bad change by returning to an earlier version |
| MERGE | Upsert: update matching rows and insert new ones |
| OPTIMIZE and VACUUM | File compaction and cleanup |

### 7.6 Gold layer: star schema

| Table | Grain and contents |
|---|---|
| Fact sales | One row per order item: keys, price, freight, line total, delivery days, order status |
| Dimension customer | Customer key, person-level id, city, state |
| Dimension product | Product key, category, weight, photo count |
| Dimension date | Date key, year, month, quarter, day of week |

The fact table joins to each dimension on a key, which keeps reporting queries simple and fast.

### 7.7 Semantic layer

A governed view defines business metrics once, so every report and the agent use the same numbers.

| Metric | Definition |
|---|---|
| Revenue | Sum of item price, delivered orders only |
| Orders | Count of distinct delivered orders |
| Average order value | Revenue divided by orders |
| Delivery days | Average days from purchase to delivery |

**Grain:** one row per year, month, state and category. The view is pre-aggregated, so a single row is not a total. Anything reading it must aggregate across rows. This detail caused most of the agent's early errors (section 11).

### 7.8 Governance with Unity Catalog

| Control | Implementation |
|---|---|
| Documentation | Table and column comments |
| Sensitive data label | The person-level customer id is tagged as PII |
| Access | Readers get SELECT on the Gold schema; Bronze and Silver stay with engineers |
| Lineage | Automatic: Silver tables feed the fact table, which feeds the metrics view |
| App access | The app's service principal can read only the Gold metrics view (least privilege) |

A tag is a label, not protection. Real protection comes from grants and, as a next step, column masking.

### 7.9 Automation with Databricks Jobs

The Job has four tasks, each depending on the previous one: bronze, silver, gold, quality checks. It supports a daily schedule or a file-arrival trigger and sends an email on failure.

The quality checks stop the Job when the data is bad:

- The fact table row count must equal the order items row count
- No null order ids
- No negative prices

---

## 8. Text-to-SQL: full explanation

### 8.1 What is Text-to-SQL

Text-to-SQL turns a plain-English question into a SQL query. A large language model (LLM) writes the query, and the database runs it. People who do not know SQL can then query data directly.

### 8.2 Why it is used here

| Problem | How Text-to-SQL helps |
|---|---|
| Managers cannot write SQL | They ask in English |
| Analysts answer the same questions repeatedly | The agent answers them automatically |
| Reports take hours | An answer arrives in seconds |
| Teams calculate revenue differently | The agent always uses the one governed view |

### 8.3 Why it is an agent, not just an LLM

A plain LLM answers from memory and can invent numbers. An agent takes steps and uses tools. This one writes SQL, validates it, runs it on real data and only then explains the result, so the numbers come from the database.

### 8.4 The five steps

```
  User question
       |
       v
 +------------------------+
 | 1. GENERATE SQL        |   LLM + description of the Gold view
 +-----------+------------+
             v
 +------------------------+
 | 2. REFUSAL CHECK       |   data not in the view? -> "I can't answer that"
 +-----------+------------+
             v
 +------------------------+
 | 3. SAFETY CHECK        |   SELECT only -> else "Blocked"
 +-----------+------------+
             v
 +------------------------+
 | 4. RUN THE QUERY       |   on retail.gold.v_sales_metrics (row limit)
 +-----------+------------+
             v
 +------------------------+
 | 5. WRITE THE ANSWER    |   LLM turns the result table into a sentence
 +-----------+------------+
             v
   "SP had the highest revenue of R$ 2,919,328.17 in 2018."

 MLflow tracing records every step above (input, output, time).
```

| Step | Name | What happens |
|---|---|---|
| 1 | Generate SQL | The LLM receives the question plus a text description of the Gold view and returns one SQL query |
| 2 | Refusal check | If the view cannot answer the question, the LLM returns a fixed refusal marker and the agent replies politely that it cannot answer |
| 3 | Safety check | Only a plain SELECT is allowed; any write or admin keyword is rejected |
| 4 | Run the query | The SQL runs on the Gold view with a row limit |
| 5 | Write the answer | The LLM turns the result table into one or two sentences, using only the returned data |

### 8.5 Worked example

| Item | Value |
|---|---|
| Question | Which state had the highest revenue in 2018? |
| Generated SQL | Sum of gross revenue grouped by state, filtered to 2018, sorted descending, limited to one row |
| Query result | SP with 2,919,328.17 |
| Final answer | SP had the highest revenue of R$ 2,919,328.17 in 2018. |

### 8.6 Prompt grounding: what the LLM is told

The LLM cannot see your database, so it receives a short description of one view. This description is the biggest factor in answer quality. It contains:

- The view's full name and every column with its type and meaning
- The grain, stating clearly that one row is not a total
- Metric rules: total revenue is the sum of revenue, total orders is the sum of orders, and average order value is total revenue divided by total orders (never an average of per-row averages)
- A rule to group by whatever the question asks about
- One worked example question with its correct SQL
- A refusal rule for anything outside the listed columns, such as sellers, profit, stock or individual customers
- Output rules: return one SQL query only, SELECT only, always use the full view name

### 8.7 Guardrails

| Guardrail | Purpose |
|---|---|
| Single read-only view | The agent can reach only the Gold metrics view |
| Safety check | Rejects anything that is not a plain SELECT and blocks insert, update, delete, drop, alter, create, merge, truncate and grant |
| Refusal marker | Refuses questions about data that does not exist instead of guessing |
| Result limit | Caps how many rows come back |
| Retry with backoff | Handles throttled LLM calls by waiting 1, 2, 4 and 8 seconds |
| Visible errors | LLM and SQL failures return a clear message instead of crashing |
| Least-privilege app access | The deployed app can read only the Gold view |

### 8.8 Model

The agent uses a Databricks-hosted instruct model (Llama 3.3 70B Instruct) through the workspace's serving endpoints, so no external API key is needed.

---

## 9. MLflow tracing and evaluation

### 9.1 Why MLflow matters

LLMs can be confidently wrong. MLflow answers two questions: what exactly did the agent do (tracing) and how good is it (evaluation). Without it the claim is "it seems to work"; with it the claim is a measured score backed by a trace of every step.

### 9.2 Tracing

Every LLM call is recorded automatically, and each function of the agent is wrapped so a question produces a trace with these steps:

| Span | What it records |
|---|---|
| Agent | The whole question and the final result |
| Generate SQL | The prompt sent and the SQL returned |
| Safety check | Whether the query was allowed |
| Run SQL | The query that was executed |
| Write answer | The result table sent in and the sentence returned |

Each span shows its input, output and time taken, so a wrong answer can be traced to the exact step that caused it.

### 9.3 Evaluation

A 20-question suite is scored automatically.

| Group | Count | Passes when |
|---|---|---|
| Answerable questions | 16 | The values from a hand-written ground-truth SQL query appear in the agent's result |
| Must-refuse questions | 4 | The agent refuses or blocks the query |

The scorer runs both the agent's SQL and the ground-truth SQL and compares the values. LLM or SQL failures count as failures, so nothing is silently skipped. Every run is saved, so two runs can be compared before and after a fix.

---

## 10. Deploying the agent as a Databricks App

**Files in the app folder**

| File | Role |
|---|---|
| app.py | Streamlit page plus the same agent logic, running queries through the SQL warehouse |
| app.yaml | Start command and the environment variable that holds the warehouse id |
| requirements.txt | Streamlit, pandas, the Databricks SDK and the Databricks OpenAI client |

**Steps**

1. Create a workspace folder and add the three files as files (not notebooks).
2. Open the Apps page through the app switcher and click Create app. Use lowercase letters, numbers and hyphens only. The name cannot be changed later.
3. Add two resources: a SQL warehouse with permission "Can use" and the Llama serving endpoint with permission "Can query". The warehouse resource key must match the one used in app.yaml.
4. Click Deploy and select the folder.
5. Grant the app's service principal access to the Gold metrics view only: use catalog, use schema on Gold, and select on the view.
6. Open the app URL once the status shows Running.

The app is available to users inside the Databricks workspace. It is not a public website, which is why the demo is shared as a video.

---

## 11. Problems found and how they were fixed

| Problem | Cause | Fix |
|---|---|---|
| Wrong or unusable answers: average order value returned many rows, and top categories raised an aggregation error | The LLM treated each pre-aggregated view row as a total | Described the grain, metric formulas and a worked example in the prompt |
| The agent answered "which seller had the most sales" with a state's revenue | No refusal path, so it substituted a different column | Added the refusal rule and test questions that expect a refusal |
| Evaluation showed a perfect score while several rows had errored | Parallel LLM calls hit the free-tier rate limit and failed rows were left out of the average | Retry with exponential backoff, one question at a time, and failures counted as failures |
| Money displayed in dollars | The data is in Brazilian reais | Told the answer prompt to use R$ |
| The app crashed on start | A newer SDK version removed the old OpenAI client helper | Switched to the Databricks OpenAI client package |
| A tag looked like protection | A tag is only a label | Documented the difference; column masking is listed as future work |

---

## 12. Results

| Measure | Result |
|---|---|
| Evaluation suite | 20 out of 20 (16 answerable, 4 must-refuse) |
| Errored rows in the final run | 0 |
| Typical time per question after the retry fix | About 4 to 5 seconds |
| Verified sample answers | SP highest revenue in 2018 (R$ 2,919,328.17); SP average order value in 2017 (R$ 124.19); 53,239 orders in 2018 |

Row counts per layer: add yours here after counting the rows in each table.

---

## 13. Limitations

- The scorer checks that the correct values appear in the result, so extra rows can still pass.
- Bronze currently overwrites tables, so loads are not incremental.
- The person-level customer id is tagged as PII but not masked. It is not exposed to the app.
- The agent answers from one view. It cannot join other tables or remember earlier questions.
- Free Edition has rate limits and app restrictions, and the app is not publicly reachable.

---

## 14. Future work


- Auto Loader and a file-arrival trigger for incremental ingestion, with MERGE into Silver
- Column masking on the person-level customer id
- Lakeflow Declarative Pipelines with built-in quality expectations
- Unity Catalog Metric Views as a first-class semantic layer
- Conversation memory and chart output in the agent
- A late-delivery prediction model tracked with the MLflow model registry

---

## Credits

Dataset: Olist Brazilian E-Commerce Public Dataset on Kaggle. Built with Databricks and MLflow.