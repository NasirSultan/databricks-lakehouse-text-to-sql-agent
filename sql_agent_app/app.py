import os, re, time
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from databricks_openai import DatabricksOpenAI

# ---------- Setup ----------
w = WorkspaceClient()
client = DatabricksOpenAI()
MODEL = "databricks-meta-llama-3-3-70b-instruct"
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

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

ANSWER_PROMPT = (
    "Answer in one or two plain sentences using ONLY the data given. "
    "Include the number. Money is in Brazilian reais (BRL), so use R$ not $. "
    "If a month number is given, name the month. Do not invent numbers."
)

# ---------- Helpers ----------
def llm(messages, max_tokens):
    for attempt in range(5):
        try:
            r = client.chat.completions.create(
                model=MODEL, messages=messages, max_tokens=max_tokens
            )
            return r.choices[0].message.content
        except Exception:
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)

def clean_sql(text):
    return text.replace("```sql", "").replace("```", "").strip()

def is_safe(sql):
    s = sql.strip().lower()
    banned = ["insert", "update", "delete", "drop", "alter",
              "create", "merge", "truncate", "grant"]
    return s.startswith("select") and not any(re.search(rf"\b{b}\b", s) for b in banned)

def run_sql(sql):
    r = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=WAREHOUSE_ID,
        wait_timeout="30s", row_limit=50,
    )
    if r.status.state != StatementState.SUCCEEDED:
        raise Exception(r.status.error.message if r.status.error else "query failed")
    cols = [c.name for c in r.manifest.schema.columns]
    return pd.DataFrame(r.result.data_array or [], columns=cols)

# ---------- Agent ----------
def ask(question):
    try:
        sql = clean_sql(llm(
            [{"role": "system", "content": SCHEMA_CONTEXT},
             {"role": "user", "content": question}], 300))
    except Exception as e:
        return "LLM_ERROR", None, f"LLM error: {str(e)[:150]}"

    if sql.upper().startswith("CANNOT_ANSWER"):
        return "CANNOT_ANSWER", None, "I can't answer that from the available sales data."
    if not is_safe(sql):
        return sql, None, "Blocked: the query was not a safe SELECT."

    try:
        df = run_sql(sql)
    except Exception as e:
        return sql, None, f"SQL error: {str(e)[:150]}"

    try:
        answer = llm(
            [{"role": "system", "content": ANSWER_PROMPT},
             {"role": "user", "content": f"Question: {question}\nResult:\n{df.to_string(index=False)}"}], 200)
    except Exception as e:
        return sql, df, f"LLM error: {str(e)[:150]}"
    return sql, df, answer.strip()

# ---------- UI ----------
st.title("Retail Sales Assistant")
st.caption("Ask a question about sales. Answers come from the governed Gold view.")

q = st.text_input("Your question", placeholder="Which state had the highest revenue in 2018?")

if st.button("Ask") and q:
    with st.spinner("Thinking..."):
        sql, df, answer = ask(q)
    st.success(answer)
    with st.expander("See the SQL"):
        st.code(sql, language="sql")
    if df is not None:
        st.dataframe(df)