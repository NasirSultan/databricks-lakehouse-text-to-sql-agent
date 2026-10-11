# Databricks notebook source
# MAGIC %pip install -U mlflow databricks-sdk openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re
import mlflow
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
client = w.serving_endpoints.get_open_ai_client()

MODEL = "databricks-meta-llama-3-3-70b-instruct"
VIEW = "retail.gold.v_sales_metrics"

mlflow.openai.autolog()   # records every LLM call in MLflow

# COMMAND ----------

SCHEMA_CONTEXT = """
You write Databricks SQL for a retail dataset. Use ONLY this view:

retail.gold.v_sales_metrics
GRAIN: one row per (year, month, customer_state, category). It is pre-aggregated,
so a single row is NOT a total. You must aggregate across rows.

Columns:
- year (int), month (int, 1-12)
- customer_state (string): Brazilian state code, e.g. SP, RJ
- category (string): product category name
- total_orders (bigint): delivered orders in that row
- gross_revenue (decimal): item revenue in that row
- avg_order_value (decimal): per-row value, DO NOT average or sum it
- avg_delivery_days (double): per-row value

Metric rules:
- Total revenue = SUM(gross_revenue)
- Total orders = SUM(total_orders)
- Average order value = SUM(gross_revenue) / SUM(total_orders)
- Always GROUP BY the dimension asked about (state, category, month, year).
- Every non-grouped column must be inside SUM(...).

Example:
Q: Which category had the most orders?
SQL: SELECT category, SUM(total_orders) AS orders FROM retail.gold.v_sales_metrics GROUP BY category ORDER BY orders DESC LIMIT 1

If the question cannot be answered with these columns (for example sellers, individual
customers, products, profit, stock, or any column not listed), return exactly:
CANNOT_ANSWER
Do not guess or substitute a different column.

Output rules:
- Return ONE SQL query only, no explanation, no markdown.
- SELECT only. Never modify data.
- Always use the full name retail.gold.v_sales_metrics.
"""

# COMMAND ----------

def clean_sql(text):
    return text.replace("```sql", "").replace("```", "").strip()

@mlflow.trace(span_type="CHAIN", name="generate_sql")
def generate_sql(question):
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SCHEMA_CONTEXT},
            {"role": "user", "content": question},
        ],
        max_tokens=300,
    )
    return clean_sql(resp.choices[0].message.content)

@mlflow.trace(span_type="TOOL", name="safety_check")
def is_safe(sql):
    s = sql.strip().lower()
    banned = ["insert", "update", "delete", "drop", "alter", "create", "merge", "truncate", "grant"]
    return s.startswith("select") and not any(re.search(rf"\b{w}\b", s) for w in banned)

@mlflow.trace(span_type="TOOL", name="run_sql")
def run_sql(sql):
    return spark.sql(sql).limit(50).toPandas()

@mlflow.trace(span_type="CHAIN", name="write_answer")
def write_answer(question, df):
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "Answer in one or two plain sentences using ONLY the data given. Include the number. Money is in Brazilian reais (BRL), so use R$ not $. If a month number is given, name the month. Do not invent numbers."},
            {"role": "user", "content": f"Question: {question}\nResult:\n{df.to_string(index=False)}"},
        ],
        max_tokens=200,
    )
    return resp.choices[0].message.content.strip()

@mlflow.trace(span_type="AGENT", name="sql_agent")
def ask(question):
    sql = generate_sql(question)
    if sql.strip().upper().startswith("CANNOT_ANSWER"):
        return {"question": question, "sql": "CANNOT_ANSWER",
                "answer": "I can't answer that from the available sales data."}
    if not is_safe(sql):
        return {"question": question, "sql": sql, "answer": "Blocked: the query was not a safe SELECT."}
    try:
        df = run_sql(sql)
    except Exception as e:
        return {"question": question, "sql": sql, "answer": f"SQL error: {str(e)[:150]}"}
    return {"question": question, "sql": sql, "answer": write_answer(question, df)}

# COMMAND ----------

tests = [
    "Which state had the highest revenue in 2018?",
    "What was the average order value for SP in 2017?",
    "Top 3 categories by revenue overall?",
    "Which month in 2018 had the most orders?",
    "Which seller had the most sales?",
]

for q in tests:
    out = ask(q)
    print("Q:", out["question"])
    print("SQL:", out["sql"])
    print("A:", out["answer"])
    print("-" * 60)

# COMMAND ----------

V = VIEW

qa = [
 ("Which state had the highest revenue in 2018?",
  f"SELECT customer_state FROM {V} WHERE year=2018 GROUP BY customer_state ORDER BY SUM(gross_revenue) DESC LIMIT 1", False),
 ("What was the average order value for SP in 2017?",
  f"SELECT SUM(gross_revenue)/SUM(total_orders) FROM {V} WHERE customer_state='SP' AND year=2017", False),
 ("Top 3 categories by revenue overall?",
  f"SELECT category FROM {V} GROUP BY category ORDER BY SUM(gross_revenue) DESC LIMIT 3", False),
 ("Which month in 2018 had the most orders?",
  f"SELECT month FROM {V} WHERE year=2018 GROUP BY month ORDER BY SUM(total_orders) DESC LIMIT 1", False),
 ("What was the total revenue in 2017?",
  f"SELECT SUM(gross_revenue) FROM {V} WHERE year=2017", False),
 ("How many orders were there in 2018?",
  f"SELECT SUM(total_orders) FROM {V} WHERE year=2018", False),
 ("Which state had the most orders overall?",
  f"SELECT customer_state FROM {V} GROUP BY customer_state ORDER BY SUM(total_orders) DESC LIMIT 1", False),
 ("What is the total revenue of the category beleza_saude?",
  f"SELECT SUM(gross_revenue) FROM {V} WHERE category='beleza_saude'", False),
 ("What was the revenue in RJ in 2018?",
  f"SELECT SUM(gross_revenue) FROM {V} WHERE customer_state='RJ' AND year=2018", False),
 ("Which month in 2017 had the highest revenue?",
  f"SELECT month FROM {V} WHERE year=2017 GROUP BY month ORDER BY SUM(gross_revenue) DESC LIMIT 1", False),
 ("Which year had higher total revenue, 2017 or 2018?",
  f"SELECT year FROM {V} WHERE year IN (2017,2018) GROUP BY year ORDER BY SUM(gross_revenue) DESC LIMIT 1", False),
 ("Top 5 states by revenue?",
  f"SELECT customer_state FROM {V} GROUP BY customer_state ORDER BY SUM(gross_revenue) DESC LIMIT 5", False),
 ("What is the overall average order value?",
  f"SELECT SUM(gross_revenue)/SUM(total_orders) FROM {V}", False),
 ("How many orders came from SP in 2017?",
  f"SELECT SUM(total_orders) FROM {V} WHERE customer_state='SP' AND year=2017", False),
 ("Which category had the most orders in 2018?",
  f"SELECT category FROM {V} WHERE year=2018 GROUP BY category ORDER BY SUM(total_orders) DESC LIMIT 1", False),
 ("What was the total revenue for MG in 2017?",
  f"SELECT SUM(gross_revenue) FROM {V} WHERE customer_state='MG' AND year=2017", False),
 # the view cannot answer these: the agent must refuse
 ("Which seller had the most sales?", "", True),
 ("What was the total profit in 2018?", "", True),
 ("Which individual customer spent the most?", "", True),
 ("How many products are currently in stock?", "", True),
]

eval_data = [{"inputs": {"question": q}, "expectations": {"sql": s, "refuse": r}} for q, s, r in qa]
print(len(eval_data), "questions")

# COMMAND ----------

import os, time

# run the evaluation one question at a time to avoid the rate limit
os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = "1"

def llm_call(messages, max_tokens, retries=5):
    for attempt in range(retries):
        try:
            resp = client.chat.completions.create(
                model=MODEL, messages=messages, max_tokens=max_tokens
            )
            return resp.choices[0].message.content
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)   # wait 1s, 2s, 4s, 8s

@mlflow.trace(span_type="CHAIN", name="generate_sql")
def generate_sql(question):
    text = llm_call(
        [{"role": "system", "content": SCHEMA_CONTEXT},
         {"role": "user", "content": question}],
        max_tokens=300,
    )
    return clean_sql(text)

@mlflow.trace(span_type="CHAIN", name="write_answer")
def write_answer(question, df):
    text = llm_call(
        [{"role": "system", "content": "Answer in one or two plain sentences using ONLY the data given. Include the number. Money is in Brazilian reais (BRL), so use R$ not $. If a month number is given, name the month. Do not invent numbers."},
         {"role": "user", "content": f"Question: {question}\nResult:\n{df.to_string(index=False)}"}],
        max_tokens=200,
    )
    return text.strip()

@mlflow.trace(span_type="AGENT", name="sql_agent")
def ask(question):
    try:
        sql = generate_sql(question)
    except Exception as e:
        return {"question": question, "sql": "LLM_ERROR", "answer": f"LLM error: {str(e)[:150]}"}
    if sql.strip().upper().startswith("CANNOT_ANSWER"):
        return {"question": question, "sql": "CANNOT_ANSWER",
                "answer": "I can't answer that from the available sales data."}
    if not is_safe(sql):
        return {"question": question, "sql": sql, "answer": "Blocked: the query was not a safe SELECT."}
    try:
        df = run_sql(sql)
    except Exception as e:
        return {"question": question, "sql": sql, "answer": f"SQL error: {str(e)[:150]}"}
    try:
        return {"question": question, "sql": sql, "answer": write_answer(question, df)}
    except Exception as e:
        return {"question": question, "sql": sql, "answer": f"LLM error: {str(e)[:150]}"}

# COMMAND ----------

@scorer
def correct(outputs, expectations):
    if not outputs or not isinstance(outputs, dict):
        return False
    sql = outputs.get("sql", "")
    answer = outputs.get("answer", "")
    refused = sql == "CANNOT_ANSWER"
    if expectations["refuse"]:
        return refused or answer.startswith("Blocked")
    if refused or sql in ("LLM_ERROR",) or sql.startswith("Blocked") \
       or answer.startswith(("SQL error", "LLM error")):
        return False
    try:
        actual = vals(spark.sql(sql).toPandas())
        expected = vals(spark.sql(expectations["sql"]).toPandas())
        return expected.issubset(actual)
    except Exception:
        return False

# COMMAND ----------

results = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=predict,
    scorers=[correct],
)

# COMMAND ----------

print(results.metrics)

df = results.result_df
print(len(df), "rows total")