#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# DQ check'leri için scenario test runner
#
# Kullanım:
#   ./tests/test_dq.sh list                  # mevcut senaryoları listeler
#   ./tests/test_dq.sh all                   # hepsini sırayla çalıştırır
#   ./tests/test_dq.sh negative_amount       # tek bir senaryoyu çalıştırır
#
# Mantık:
#   1) Hedef tabloyu temiz duruma getir (DAG'ın load adımını tetikle)
#   2) Senaryo: SQL ile bilerek kötü veri enjekte et
#   3) İlgili check task'ını "airflow tasks test" ile izole çalıştır
#   4) Beklenen sonuçla (fail/pass) karşılaştır → ✅ veya ❌
#   5) Cleanup: hedef tabloyu yeniden yükle, bir sonrakine hazırla
#
# Önkoşul: docker compose ayakta, dq_etl_dag yüklü, source'da seed var.
# ----------------------------------------------------------------------------

set -uo pipefail

DAG_ID="dq_etl_dag"
DATE="$(date -I)"   # bugünün tarihi YYYY-MM-DD

# Renkler
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASS_COUNT=0
FAIL_COUNT=0

# ----------------------------------------------------------------------------
# Helper fonksiyonlar
# ----------------------------------------------------------------------------

psql_w() {
    docker compose exec -T warehouse-db psql -U dwh -d warehouse -v ON_ERROR_STOP=1 -c "$1"
}

airflow_test() {
    # airflow tasks test → tek task'ı izole çalıştır
    # exit 0 = task başarılı, exit !=0 = task başarısız
    local task=$1
    docker compose exec -T airflow \
        airflow tasks test "$DAG_ID" "$task" "$DATE" \
        > /tmp/dq_test.log 2>&1
}

# Bir scenario'nun beklenen sonucu üretip üretmediğini kontrol eder
assert_outcome() {
    local scenario=$1
    local task=$2
    local expected=$3   # "fail" veya "pass"

    if airflow_test "$task"; then
        actual="pass"
    else
        actual="fail"
    fi

    if [[ "$actual" == "$expected" ]]; then
        echo -e "  ${GREEN}✅ PASS${NC}  [$scenario] task=$task → expected=$expected got=$actual"
        PASS_COUNT=$((PASS_COUNT+1))
        return 0
    else
        echo -e "  ${RED}❌ FAIL${NC}  [$scenario] task=$task → expected=$expected got=$actual"
        echo -e "  ${YELLOW}--- son 20 satır log ---${NC}"
        tail -n 20 /tmp/dq_test.log | sed 's/^/    /'
        FAIL_COUNT=$((FAIL_COUNT+1))
        return 1
    fi
}

# Hedefi temiz duruma getir: today partition sil + DAG'ın load adımını tetikle
reseed_target() {
    psql_w "DELETE FROM dwh.fact_transactions WHERE event_date = '$DATE';" > /dev/null
    airflow_test load_to_target > /dev/null 2>&1 || true
}

# Source'u temiz duruma getir (test için bozduysak)
# NOT: Test ortamında natural NULL üretmiyoruz ki source_schema_ok happy path'te
#      pass etsin. NULL'u test scenario'su açıkça enjekte ediyor.
reseed_source_today() {
    psql_w "DELETE FROM source_db.raw_transactions WHERE event_date = '$DATE';" > /dev/null
    psql_w "
      INSERT INTO source_db.raw_transactions
        (transaction_id, event_date, customer_id, amount, currency, status)
      SELECT
        1000000 + t.n,
        '$DATE'::date,
        (1000 + (random() * 2000)::int),
        (random() * 500 + 10)::numeric(14,2),
        (ARRAY['USD','EUR','TRY','GBP'])[1 + (random()*3)::int],
        (ARRAY['active','completed','refunded','pending','cancelled'])[1 + (random()*4)::int]
      FROM generate_series(1, 5000) AS t(n);
    " > /dev/null
}

print_header() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# ----------------------------------------------------------------------------
# Bootstrap: DAG'ın bir kez tam çalışmış olması lazım — değilse load yapalım
# ----------------------------------------------------------------------------
bootstrap() {
    print_header "Bootstrap: source ve hedef tabloyu temiz duruma getir"
    # Init SQL'i başka bir günde çalışmış olabilir → source'da bugün için
    # veri olmayabilir. Her bootstrap'ta bugünü yeniden seed et.
    reseed_source_today
    reseed_target
    local s_count t_count
    s_count=$(psql_w "SELECT COUNT(*) FROM source_db.raw_transactions WHERE event_date = '$DATE';" \
              | grep -E '^\s+[0-9]+' | tr -d ' ')
    t_count=$(psql_w "SELECT COUNT(*) FROM dwh.fact_transactions WHERE event_date = '$DATE';" \
              | grep -E '^\s+[0-9]+' | tr -d ' ')
    echo "  Source'ta bugün $s_count satır, target'ta $t_count satır."
}

# ============================================================================
# SCENARIOS
# ============================================================================

# --- Source-side scenarios ---

test_source_empty() {
    print_header "Scenario: source_empty (source'a bugün hiç veri gelmemiş)"
    psql_w "DELETE FROM source_db.raw_transactions WHERE event_date = '$DATE';" > /dev/null
    assert_outcome "source_empty" "source_has_data" "fail"
    reseed_source_today
}

test_source_required_null() {
    print_header "Scenario: source_required_null (amount kolonunda NULL var)"
    # source_schema_ok check'i transaction_id, event_date, amount NOT NULL ister
    # Burada amount'a NULL koyup check'in yakalayıp yakalamadığını görüyoruz
    psql_w "
      UPDATE source_db.raw_transactions
      SET amount = NULL
      WHERE event_date = '$DATE'
        AND ctid IN (SELECT ctid FROM source_db.raw_transactions
                     WHERE event_date = '$DATE' LIMIT 5);
    " > /dev/null
    assert_outcome "source_required_null" "source_schema_ok" "fail"
    reseed_source_today
}

# --- Target column-level scenarios ---

test_pk_duplicate() {
    print_header "Scenario: pk_duplicate (target'ta duplicate transaction_id)"
    psql_w "
      INSERT INTO dwh.fact_transactions
      SELECT * FROM dwh.fact_transactions
      WHERE event_date = '$DATE' LIMIT 5;
    " > /dev/null
    assert_outcome "pk_duplicate" "target_column_checks" "fail"
    reseed_target
}

test_amount_null() {
    print_header "Scenario: amount_null (target'ta amount NULL'lanmiş)"
    # Target'ta amount NOT NULL constraint'i var, geçici olarak kaldırıyoruz
    # ki check'in yakalama kabiliyetini test edebilelim. Sonra geri koyuyoruz.
    psql_w "ALTER TABLE dwh.fact_transactions ALTER COLUMN amount DROP NOT NULL;" > /dev/null
    psql_w "
      UPDATE dwh.fact_transactions
      SET amount = NULL
      WHERE event_date = '$DATE'
        AND ctid IN (SELECT ctid FROM dwh.fact_transactions
                     WHERE event_date = '$DATE' LIMIT 1);
    " > /dev/null
    assert_outcome "amount_null" "target_column_checks" "fail"
    # Cleanup: NULL'u sil → constraint'i geri koy → reseed
    psql_w "DELETE FROM dwh.fact_transactions WHERE event_date = '$DATE' AND amount IS NULL;" > /dev/null
    psql_w "ALTER TABLE dwh.fact_transactions ALTER COLUMN amount SET NOT NULL;" > /dev/null
    reseed_target
}

test_amount_negative() {
    print_header "Scenario: amount_negative (negatif tutar enjekte)"
    psql_w "
      UPDATE dwh.fact_transactions
      SET amount = -10
      WHERE event_date = '$DATE'
        AND ctid IN (SELECT ctid FROM dwh.fact_transactions
                     WHERE event_date = '$DATE' LIMIT 1);
    " > /dev/null
    assert_outcome "amount_negative" "target_column_checks" "fail"
    reseed_target
}

# --- Target table-level scenarios ---

test_invalid_status() {
    print_header "Scenario: invalid_status (enum dışı status değeri)"
    psql_w "
      UPDATE dwh.fact_transactions
      SET status = 'banana'
      WHERE event_date = '$DATE'
        AND ctid IN (SELECT ctid FROM dwh.fact_transactions
                     WHERE event_date = '$DATE' LIMIT 1);
    " > /dev/null
    assert_outcome "invalid_status" "target_table_checks" "fail"
    reseed_target
}

test_row_count_too_low() {
    print_header "Scenario: row_count_too_low (target'ta yeterince satır yok)"
    psql_w "
      DELETE FROM dwh.fact_transactions
      WHERE event_date = '$DATE'
        AND ctid IN (SELECT ctid FROM dwh.fact_transactions
                     WHERE event_date = '$DATE' OFFSET 100);
    " > /dev/null
    assert_outcome "row_count_too_low" "target_table_checks" "fail"
    reseed_target
}

# --- Reconciliation scenarios ---

test_recon_count_mismatch() {
    print_header "Scenario: recon_count_mismatch (target'tan satır kaybedildi)"
    psql_w "
      DELETE FROM dwh.fact_transactions
      WHERE event_date = '$DATE'
        AND ctid IN (SELECT ctid FROM dwh.fact_transactions
                     WHERE event_date = '$DATE' LIMIT 200);
    " > /dev/null
    assert_outcome "recon_count_mismatch" "recon_row_count" "fail"
    reseed_target
}

test_recon_sum_mismatch() {
    print_header "Scenario: recon_sum_mismatch (amount değerleri kaymış)"
    psql_w "
      UPDATE dwh.fact_transactions
      SET amount = amount * 100
      WHERE event_date = '$DATE'
        AND ctid IN (SELECT ctid FROM dwh.fact_transactions
                     WHERE event_date = '$DATE' LIMIT 50);
    " > /dev/null
    assert_outcome "recon_sum_mismatch" "recon_amount_sum" "fail"
    reseed_target
}

# --- Happy path: temiz veride pass etmeli ---

test_happy_path() {
    print_header "Scenario: happy_path (her şey temiz, hiçbir check fail etmemeli)"
    reseed_target
    assert_outcome "happy_path" "target_column_checks" "pass"
    assert_outcome "happy_path" "target_table_checks"  "pass"
    assert_outcome "happy_path" "recon_row_count"      "pass"
    assert_outcome "happy_path" "recon_amount_sum"     "pass"
}

# ============================================================================
# Dispatch
# ============================================================================

ALL_TESTS=(
    test_happy_path
    test_source_empty
    test_source_required_null
    test_pk_duplicate
    test_amount_null
    test_amount_negative
    test_invalid_status
    test_row_count_too_low
    test_recon_count_mismatch
    test_recon_sum_mismatch
)

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "  Toplam: ${GREEN}$PASS_COUNT pass${NC}, ${RED}$FAIL_COUNT fail${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    [[ $FAIL_COUNT -eq 0 ]] && exit 0 || exit 1
}

cmd="${1:-all}"

case "$cmd" in
    list)
        echo "Mevcut senaryolar:"
        for t in "${ALL_TESTS[@]}"; do
            echo "  - ${t#test_}"
        done
        ;;
    all)
        bootstrap
        for t in "${ALL_TESTS[@]}"; do
            "$t"
        done
        print_summary
        ;;
    *)
        # Tek senaryo: ./test_dq.sh negative_amount
        fn="test_$cmd"
        if declare -f "$fn" > /dev/null; then
            bootstrap
            "$fn"
            print_summary
        else
            echo "Bilinmeyen senaryo: $cmd"
            echo "Listelemek için: $0 list"
            exit 2
        fi
        ;;
esac