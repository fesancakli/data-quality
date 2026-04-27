"""
Data Quality odaklı ETL DAG örneği
-----------------------------------
Akış: source_db.raw_transactions  -->  dwh.fact_transactions

Bu DAG, daha önce konuştuğumuz DQ pattern'ini somut olarak uygular:
  1) Source pre-check       -> veri geldi mi, şekli bozuk mu?
  2) Extract + Load         -> asıl iş
  3) Target post-check      -> null / unique / range kontrolleri
  4) Reconciliation         -> source vs target count & sum tutarlılığı
  5) Interval (drift) check -> bugünkü metrikler dünle tutarlı mı?
  6) DQ metadata logging    -> tüm metrikler bir tabloya yazılır
  7) Notify / finalize      -> başarı / başarısızlık bildirimi

Gereksinim:
  - apache-airflow >= 2.7
  - apache-airflow-providers-common-sql
  - Bir SQL bağlantısı:  Airflow Connection id = "warehouse"
"""

from __future__ import annotations

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.models.baseoperator import cross_downstream
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
    SQLIntervalCheckOperator,
    SQLThresholdCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Konfigürasyon
# ---------------------------------------------------------------------------
CONN_ID = "warehouse"                       # Airflow connection
SOURCE_TABLE = "source_db.raw_transactions"
TARGET_TABLE = "dwh.fact_transactions"
DQ_METRICS_TABLE = "dwh.dq_metrics"
DATE_COL = "event_date"
PK_COL = "transaction_id"

# Threshold'lar — business ile netleştirilmesi gereken sayılar
MIN_DAILY_ROWS = 1_000          # hard floor: bu kadar satır gelmeliyse gelmiyor, patla
MAX_NULL_RATIO = 0.01            # %1 üstü null kabul edilmez (kritik kolonlar)
MAX_DUPLICATE_RATIO = 0.0        # duplicate PK hiç olmamalı
RECON_TOLERANCE = 0.001          # source/target arasında %0.1 fark tolere edilir

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


# ---------------------------------------------------------------------------
# DQ metadata tablosuna metrik yazan yardımcı
# ---------------------------------------------------------------------------
def log_dq_metric(metric_name: str, value: float, status: str, threshold: float | None = None):
    """XCom + metadata tablosuna DQ sonucu yazar.

    Gerçek hayatta bir hook kullanıp INSERT edilir; burada pseudo-SQL bırakıyorum.
    """
    from airflow.providers.common.sql.hooks.sql import DbApiHook

    hook = DbApiHook.get_hook(conn_id=CONN_ID)
    hook.run(
        f"""
        INSERT INTO {DQ_METRICS_TABLE}
            (dag_id, run_date, metric_name, metric_value, threshold, status, created_at)
        VALUES
            ('dq_etl_dag', '{{{{ ds }}}}', %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """,
        parameters=(metric_name, value, threshold, status),
    )


# ---------------------------------------------------------------------------
# DAG tanımı
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dq_etl_dag",
    description="Source -> target ETL, DQ checks ile",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",            # her gün 03:00
    catchup=False,
    max_active_runs=1,
    tags=["etl", "data-quality", "example"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -----------------------------------------------------------------------
    # 1) SOURCE PRE-CHECK
    # Source'da en az N satır var mı? Şema beklediğimiz gibi mi?
    # -----------------------------------------------------------------------
    source_has_data = SQLCheckOperator(
        task_id="source_has_data",
        conn_id=CONN_ID,
        sql=f"""
            SELECT CASE
                     WHEN COUNT(*) >= {MIN_DAILY_ROWS} THEN 1
                     ELSE 0
                   END
            FROM {SOURCE_TABLE}
            WHERE {DATE_COL} = '{{{{ ds }}}}'
        """,
    )

    source_schema_ok = SQLTableCheckOperator(
        task_id="source_schema_ok",
        conn_id=CONN_ID,
        table=SOURCE_TABLE,
        checks={
            # Beklediğimiz kolonlar mevcut & dolu mu
            "required_columns_present": {
                "check_statement": f"""
                    {PK_COL} IS NOT NULL
                    AND {DATE_COL} IS NOT NULL
                    AND amount IS NOT NULL
                """,
            },
        },
        partition_clause=f"{DATE_COL} = '{{{{ ds }}}}'",
    )

    # -----------------------------------------------------------------------
    # 2) EXTRACT + LOAD
    # Idempotent load: aynı partition'ı önce sil, sonra yaz.
    # -----------------------------------------------------------------------
    delete_target_partition = SQLExecuteQueryOperator(
        task_id="delete_target_partition",
        conn_id=CONN_ID,
        sql=f"DELETE FROM {TARGET_TABLE} WHERE {DATE_COL} = '{{{{ ds }}}}';",
    )

    load_to_target = SQLExecuteQueryOperator(
        task_id="load_to_target",
        conn_id=CONN_ID,
        sql=dedent(f"""
            INSERT INTO {TARGET_TABLE} (
                {PK_COL}, {DATE_COL}, customer_id, amount, currency,
                status, created_at, load_ts
            )
            SELECT
                {PK_COL},
                {DATE_COL},
                customer_id,
                amount,
                UPPER(currency)       AS currency,
                status,
                created_at,
                CURRENT_TIMESTAMP     AS load_ts
            FROM {SOURCE_TABLE}
            WHERE {DATE_COL} = '{{{{ ds }}}}'
              AND amount IS NOT NULL
              AND status IN ('active', 'completed', 'refunded');
        """),
    )

    # -----------------------------------------------------------------------
    # 3) POST-CHECK: kolon bazlı sağlık
    # Null oranı, unique PK, değer aralığı, enum uyumu
    # -----------------------------------------------------------------------
    column_checks = SQLColumnCheckOperator(
        task_id="target_column_checks",
        conn_id=CONN_ID,
        table=TARGET_TABLE,
        partition_clause=f"{DATE_COL} = '{{{{ ds }}}}'",
        column_mapping={
            PK_COL: {
                "null_check":    {"equal_to": 0},
                # unique_check arka planda COUNT(*) - COUNT(DISTINCT col) hesaplar
                # Sıfır => duplicate yok. Sıfır değilse duplicate var demektir.
                "unique_check":  {"equal_to": 0},
            },
            "customer_id": {
                "null_check": {"equal_to": 0},
            },
            "amount": {
                "null_check": {"equal_to": 0},
                "min":        {"geq_to": 0},          # negatif tutar olmaz
            },
            "currency": {
                "null_check":     {"equal_to": 0},
                "distinct_check": {"leq_to": 10},     # max 10 farklı para birimi
            },
        },
    )

    # -----------------------------------------------------------------------
    # 4) TABLE-LEVEL: iş kuralları
    # -----------------------------------------------------------------------
    table_checks = SQLTableCheckOperator(
        task_id="target_table_checks",
        conn_id=CONN_ID,
        table=TARGET_TABLE,
        partition_clause=f"{DATE_COL} = '{{{{ ds }}}}'",
        checks={
            "row_count_above_floor": {
                "check_statement": f"COUNT(*) >= {MIN_DAILY_ROWS}",
            },
            "no_future_dates": {
                "check_statement": f"MAX({DATE_COL}) <= CURRENT_DATE",
            },
            "status_values_valid": {
                "check_statement": "SUM(CASE WHEN status NOT IN ('active','completed','refunded') THEN 1 ELSE 0 END) = 0",
            },
        },
    )

    # -----------------------------------------------------------------------
    # 5) RECONCILIATION: source vs target
    # Filtrelediğimiz kayıtları çıkararak count & sum tutuyor mu?
    # -----------------------------------------------------------------------
    recon_row_count = SQLThresholdCheckOperator(
        task_id="recon_row_count",
        conn_id=CONN_ID,
        sql=f"""
            SELECT ABS(
                (SELECT COUNT(*) FROM {SOURCE_TABLE}
                  WHERE {DATE_COL} = '{{{{ ds }}}}'
                    AND amount IS NOT NULL
                    AND status IN ('active','completed','refunded'))
              - (SELECT COUNT(*) FROM {TARGET_TABLE}
                  WHERE {DATE_COL} = '{{{{ ds }}}}')
            ) * 1.0
            / NULLIF((SELECT COUNT(*) FROM {SOURCE_TABLE}
                       WHERE {DATE_COL} = '{{{{ ds }}}}'), 0)
        """,
        min_threshold=0,
        max_threshold=RECON_TOLERANCE,
    )

    recon_amount_sum = SQLThresholdCheckOperator(
        task_id="recon_amount_sum",
        conn_id=CONN_ID,
        sql=f"""
            SELECT ABS(
                (SELECT COALESCE(SUM(amount),0) FROM {SOURCE_TABLE}
                  WHERE {DATE_COL} = '{{{{ ds }}}}'
                    AND amount IS NOT NULL
                    AND status IN ('active','completed','refunded'))
              - (SELECT COALESCE(SUM(amount),0) FROM {TARGET_TABLE}
                  WHERE {DATE_COL} = '{{{{ ds }}}}')
            ) * 1.0
            / NULLIF((SELECT SUM(amount) FROM {SOURCE_TABLE}
                       WHERE {DATE_COL} = '{{{{ ds }}}}'), 0)
        """,
        min_threshold=0,
        max_threshold=RECON_TOLERANCE,
    )

    # -----------------------------------------------------------------------
    # 6) INTERVAL (DRIFT) CHECK
    # Bugün ile 7 gün önce arasında row_count / avg_amount çok mu saptı?
    # -----------------------------------------------------------------------
    drift_check = SQLIntervalCheckOperator(
        task_id="drift_check_vs_last_week",
        conn_id=CONN_ID,
        table=TARGET_TABLE,
        days_back=-7,
        date_filter_column=DATE_COL,
        metrics_thresholds={
            "COUNT(*)":        1.5,   # volume en fazla %50 sapabilir
            "AVG(amount)":     1.2,   # ortalama tutar en fazla %20 sapabilir
            "COUNT(DISTINCT customer_id)": 1.3,
        },
    )

    # -----------------------------------------------------------------------
    # 7) FRESHNESS CHECK
    # -----------------------------------------------------------------------
    freshness_check = SQLCheckOperator(
        task_id="freshness_check",
        conn_id=CONN_ID,
        sql=f"""
            SELECT CASE
                WHEN MAX(load_ts) >= CURRENT_TIMESTAMP - INTERVAL '6 hours' THEN 1
                ELSE 0
            END
            FROM {TARGET_TABLE}
        """,
    )

    # -----------------------------------------------------------------------
    # 8) DQ METADATA LOGGING
    # Tüm checklerin özetini metadata tablosuna yaz — trend / dashboard için
    # -----------------------------------------------------------------------
    def persist_dq_summary(**context):
        """Tüm DQ metriklerini tek seferde metadata tablosuna yazar."""
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        hook = DbApiHook.get_hook(conn_id=CONN_ID)
        ds = context["ds"]
        run_id = context["run_id"]

        # Metrikleri tek query ile topla
        rows = hook.get_records(f"""
            SELECT
                'target_row_count'                             AS metric_name,
                COUNT(*)                                        AS metric_value
            FROM {TARGET_TABLE} WHERE {DATE_COL} = '{ds}'
            UNION ALL
            SELECT 'target_distinct_customers', COUNT(DISTINCT customer_id)
            FROM {TARGET_TABLE} WHERE {DATE_COL} = '{ds}'
            UNION ALL
            SELECT 'target_sum_amount', COALESCE(SUM(amount),0)
            FROM {TARGET_TABLE} WHERE {DATE_COL} = '{ds}'
            UNION ALL
            SELECT 'target_null_customer_ratio',
                   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0)
            FROM {TARGET_TABLE} WHERE {DATE_COL} = '{ds}'
        """)

        for metric_name, metric_value in rows:
            hook.run(
                f"""
                INSERT INTO {DQ_METRICS_TABLE}
                    (dag_id, run_id, run_date, metric_name, metric_value, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                parameters=("dq_etl_dag", run_id, ds, metric_name, float(metric_value or 0), "OK"),
            )

    persist_metrics = PythonOperator(
        task_id="persist_dq_summary",
        python_callable=persist_dq_summary,
        trigger_rule=TriggerRule.ALL_DONE,   # checkler fail etse bile metrikler yazılsın
    )

    # -----------------------------------------------------------------------
    # 9) NOTIFY
    # -----------------------------------------------------------------------
    notify_success = EmptyOperator(
        task_id="notify_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure = EmptyOperator(
        task_id="notify_failure",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # -----------------------------------------------------------------------
    # Dependencies
    # -----------------------------------------------------------------------
    start >> [source_has_data, source_schema_ok]
    [source_has_data, source_schema_ok] >> delete_target_partition >> load_to_target

    load_to_target >> [column_checks, table_checks]

    # list >> list Python'da olmuyor; cross_downstream ile her iki check'in
    # her iki recon task'ına bağlanmasını sağlıyoruz (2x2 = 4 dependency).
    cross_downstream(
        from_tasks=[column_checks, table_checks],
        to_tasks=[recon_row_count, recon_amount_sum],
    )

    [recon_row_count, recon_amount_sum] >> drift_check >> freshness_check

    freshness_check >> persist_metrics
    persist_metrics >> [notify_success, notify_failure] >> end