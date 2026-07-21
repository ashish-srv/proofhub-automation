import os
import psycopg2
import pandas as pd
import json
import ast
# ============================================================
# DATABASE URL
# ============================================================
DATABASE_URL = os.environ.get("QUOTATIONS_DATABASE_URL")

if not DATABASE_URL:
    print("❌ QUOTATIONS_DATABASE_URL environment variable not set.")
    exit()
# ============================================================
# Connect
# ============================================================
conn = psycopg2.connect(DATABASE_URL)
# ============================================================
# Read quotations table
# ============================================================
query = """
SELECT *
FROM quotations;
"""
data = pd.read_sql(query, conn)
conn.close()
print(f"Total quotations : {len(data)}")
# ============================================================
# Parse payload
# ============================================================
output = []
for _, quotation in data.iterrows():
    payload = quotation["payload"]
    # --------------------------------------------------------
    # Convert payload into dictionary
    # --------------------------------------------------------
    try:
        if isinstance(payload, dict):
            payload_dict = payload
        elif isinstance(payload, str):
            try:
                payload_dict = json.loads(payload)
            except:
                payload_dict = ast.literal_eval(payload)
        else:
            continue
    except Exception:
        continue
    rows = payload_dict.get("rows", [])
    # ========================================================
    # Check each item
    # ========================================================
    for item in rows:
        core = item.get("coreDept", "")
        sub = item.get("subDept", "")
        fmt = item.get("creativeFormat", "")
        creatives = item.get("noOfCreatives")
        # ----------------------------------------------------
        # BRAND COMMUNICATION (COPY)
        # ----------------------------------------------------
        if (
            core == "Brand Communication (Copy)"
            and sub == "Copy"
            and fmt == "LP Content"
        ):
            record = quotation.drop(labels=["payload"]).to_dict()
            record["Department"] = core
            record["Sub Department"] = sub
            record["Format"] = fmt
            record["Creatives"] = creatives
            output.append(record)
        # ----------------------------------------------------
        # DESIGNING
        # ----------------------------------------------------
        elif (
            core == "Designing"
            and sub == "Design"
            and fmt != "Adsets"
        ):
            record = quotation.drop(labels=["payload"]).to_dict()
            record["Department"] = core
            record["Sub Department"] = sub
            record["Format"] = fmt
            record["Creatives"] = creatives
            output.append(record)
# ============================================================
# Export
# ============================================================
result = pd.DataFrame(output)
result.to_csv(
    "Quotation_Required_Data.csv",
    index=False,
    encoding="utf-8-sig"
)
print("\nDone!")
print(f"Rows exported : {len(result)}")
print("File : Quotation_Required_Data.csv")
