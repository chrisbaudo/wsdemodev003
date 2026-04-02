# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e513c730-c3e9-4ee4-9417-e402e73abdf2",
# META       "default_lakehouse_name": "EnrollmentSilver",
# META       "default_lakehouse_workspace_id": "d2a440f4-6d00-4354-9f77-a2fcae66e547",
# META       "known_lakehouses": [
# META         {
# META           "id": "e513c730-c3e9-4ee4-9417-e402e73abdf2"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "4efcdcfe-00c1-a03e-4c6e-f0f7fbe74428",
# META       "known_warehouses": [
# META         {
# META           "id": "4efcdcfe-00c1-a03e-4c6e-f0f7fbe74428",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Discover Lakehouse Tables

# CELL ********************

# -------------------------------------------------------------------
# PARAMETERS — set these for your environment
# -------------------------------------------------------------------
WAREHOUSE_NAME = "EnrollmentGold"   # target Fabric Warehouse item name
TARGET_SCHEMA  = "dbo"           # Warehouse schema to write into
TABLE_NAME_PREFIX = ""           # e.g., "stg_" if you want to prefix target tables

# Optional: a simple allow/deny filter (names are case-insensitive)
INCLUDE_TABLES = None            # e.g., {"Customers","Orders"} or None for all
EXCLUDE_TABLES = set()           # e.g., {"_internal_audit"}

# -------------------------------------------------------------------
# DISCOVER LAKEHOUSE TABLES
# The attached Lakehouse is the current Spark database (Hive metastore).
# -------------------------------------------------------------------
current_db = spark.catalog.currentDatabase()
all_tbls   = [t for t in spark.catalog.listTables(dbName=current_db) if t.tableType.lower() in ("managed", "external")]

def should_process(name: str) -> bool:
    n = name.lower()
    if INCLUDE_TABLES and n not in {x.lower() for x in INCLUDE_TABLES}:
        return False
    if n in {x.lower() for x in EXCLUDE_TABLES}:
        return False
    return True

tables_to_process = [t for t in all_tbls if should_process(t.name)]
print(f"Discovered {len(tables_to_process)} Lakehouse tables in database '{current_db}'.")
for t in tables_to_process:
    print("  -", t.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load to Warehouse Tables

# CELL ********************

# -------------------------------------------------------------------
# TRANSFORM + LOAD LOOP
# Reads each Lakehouse table, applies transformations, writes to Warehouse
# -------------------------------------------------------------------
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


errors = []

for t in tables_to_process:
    source_fqn   = f"{current_db}.{t.name}"   # Lakehouse catalog reference
    target_table = f"{TABLE_NAME_PREFIX}{t.name}"  # Warehouse table name

    try:
        # 1) READ from Lakehouse (Delta)
        df = spark.read.table(source_fqn)  # equivalent to SELECT * FROM <db>.<table>

        # 2) TRANSFORM (currently: select all columns + add lineage/ingest_ts)
        df_transformed = (
            df.select("*")  # <-- "select all columns" explicitly
              .withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_source_table", F.lit(t.name))
        )

        # 3) WRITE to Warehouse via connector
        #    Uses synapsesql("<Warehouse>.<schema>.<table>")
        target_fqn = f"{WAREHOUSE_NAME}.{TARGET_SCHEMA}.{target_table}"
        df_transformed.write.mode("overwrite").synapsesql(target_fqn) # this uses default mode - errorifexists

        print(f"Loaded Warehouse table '{t.name}' -> Warehouse '{target_fqn}'")

    except Exception as ex:
        msg = f"[ERROR] {t.name}: {ex}"
        errors.append(msg)
        print(msg)

if errors:
    print("\nCompleted with errors on these tables:")
    for e in errors:
        print(" -", e)
else:
    print("\nAll tables loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Visualize Enrollments

# CELL ********************

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

import plotly.express as px

df = spark.table("enrollments").toPandas()
course_counts = df['course_id'].value_counts().reset_index()
course_counts.columns = ['course_id', 'enrollment_count']

fig = px.bar(
    course_counts.head(20),  # Show top 20 courses for readability
    x='course_id',
    y='enrollment_count',
    template='plotly_dark',
    title='Top 20 Courses by Enrollment Count'
)
fig.update_layout(xaxis_title='Course ID', yaxis_title='Enrollment Count')
fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
