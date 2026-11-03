
#!/usr/bin/env bash

set -euo pipefail

# Resolve project directory automatically
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_DIR="$PROJECT_DIR/Kafka"
AIRFLOW_DIR="$PROJECT_DIR/Airflow"

RUNTIME_DIR="$PROJECT_DIR/.runtime"
mkdir -p "$RUNTIME_DIR"

# Check whether a tracked PID belongs to a running Mintty terminal.
# Find the actual Windows PID of a Mintty window by its title.
find_mintty_pid() {
    local title="$1"

    MINTTY_TITLE="$title" powershell.exe -NoProfile -Command '
        $proc = Get-CimInstance Win32_Process |
            Where-Object {
                $_.Name -eq "mintty.exe" -and
                $_.CommandLine -like "*--title=$env:MINTTY_TITLE*"
            } |
            Select-Object -First 1

        if ($proc) {
            [Console]::WriteLine($proc.ProcessId)
        }
    ' | tr -d '\r\n'
}

# Check whether the Windows PID is an active Mintty process.
is_mintty_running() {
    local pid="$1"

    MINTTY_PID="$pid" powershell.exe -NoProfile -Command '
        $proc = Get-Process -Id ([int]$env:MINTTY_PID) -ErrorAction SilentlyContinue

        if ($proc -and $proc.ProcessName -eq "mintty") {
            exit 0
        }

        exit 1
    ' >/dev/null 2>&1
}

echo "=========================================="
echo " COMMODITY MARKET STREAMING PIPELINE"
echo "=========================================="

# Check required applications
# for cmd in docker python mintty curl; do
# for cmd in docker python mintty curl tasklist.exe taskkill.exe; do
for cmd in docker python mintty curl powershell.exe taskkill.exe; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: $cmd is not available."
        exit 1
    fi
done

# Check Docker Desktop
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker Desktop is not running."
    exit 1
fi

# Check local environment files
for file in "$KAFKA_DIR/.env" "$AIRFLOW_DIR/.env"; do
    if [[ ! -f "$file" ]]; then
        echo "ERROR: Missing environment file: $file"
        exit 1
    fi
done

# Prevent duplicate consumer/producer terminals.
for component in consumer producer; do

    if [[ "$component" == "consumer" ]]; then
        TITLE="Commodity Consumer"
    else
        TITLE="Commodity Producer"
    fi

    EXISTING_PID="$(find_mintty_pid "$TITLE")"

    if [[ "$EXISTING_PID" =~ ^[0-9]+$ ]]; then
        echo "ERROR: $TITLE terminal is already running."
        echo "Run ./stop-project.sh before starting another instance."
        exit 1
    fi

    rm -f "$RUNTIME_DIR/${component}-terminal.pid"
done

# Ensure external Zookeeper volumes exist
docker volume create commodity-kafka_zookeeper_data >/dev/null
docker volume create commodity-kafka_zookeeper_log_data >/dev/null

# Start Kafka infrastructure
echo
echo "[1/4] Starting Kafka infrastructure..."

(
    cd "$KAFKA_DIR"
    docker compose -p commodity-kafka up -d
)

# Wait until Kafka reports healthy
echo
echo "Waiting for Kafka broker to become healthy..."

KAFKA_HEALTH=""

for attempt in {1..60}; do
    KAFKA_ID="$(
        cd "$KAFKA_DIR"
        docker compose -p commodity-kafka ps -q kafka
    )"

    if [[ -n "$KAFKA_ID" ]]; then
        KAFKA_HEALTH="$(
            docker inspect \
                --format '{{.State.Health.Status}}' \
                "$KAFKA_ID" 2>/dev/null || true
        )"
    fi

    if [[ "$KAFKA_HEALTH" == "healthy" ]]; then
        break
    fi

    sleep 2
done

if [[ "$KAFKA_HEALTH" != "healthy" ]]; then
    echo "ERROR: Kafka did not become healthy."
    echo "Check Kafka logs using:"
    echo "cd Kafka && docker compose -p commodity-kafka logs kafka"
    exit 1
fi

echo "Kafka is healthy!"

# Start Airflow infrastructure
echo
echo "[2/4] Starting Airflow infrastructure..."

(
    cd "$AIRFLOW_DIR"
    docker compose -p commodity-airflow up -d
)

echo "Airflow containers started."


# Wait for Airflow UI and generated admin password
echo
echo "Waiting for Airflow initialization..."

AIRFLOW_READY=false

for attempt in {1..60}; do

    if docker exec commodity-airflow-airflow-1 \
        sh -c 'test -s "${AIRFLOW_HOME:-/opt/airflow}/simple_auth_manager_passwords.json.generated"' \
        >/dev/null 2>&1 \
        && curl -fsS http://localhost:8080/ >/dev/null 2>&1; then

        AIRFLOW_READY=true
        break
    fi

    # echo "Waiting for Airflow... ($attempt/60)"
    if (( attempt % 3 == 0 )); then
        echo "Waiting for Airflow... ($attempt/60)"
    fi
    sleep 5
done

if [[ "$AIRFLOW_READY" != true ]]; then
    echo "ERROR: Airflow did not become ready."
    echo "Check Airflow logs:"
    docker logs --tail 50 commodity-airflow-airflow-1
    exit 1
fi

echo
echo "=========================================="
echo " AIRFLOW LOGIN CREDENTIALS"
echo "=========================================="
echo "Airflow URL: http://localhost:8080"
echo "Username: admin"
echo "Generated password:"

docker exec commodity-airflow-airflow-1 \
    sh -c 'cat "${AIRFLOW_HOME:-/opt/airflow}/simple_auth_manager_passwords.json.generated"'

echo "=========================================="
echo "Save your password securely."
echo


# Launch the consumer in a separate Git Bash window
echo
echo "[3/4] Starting commodity consumer..."

mintty --title="Commodity Consumer" bash -lc '
    cd "$1" || exit 1

    set -a
    source .env
    set +a

    echo "Starting commodity consumer..."
    python -u commodity_consumer.py

    status=$?
    echo
    echo "Consumer exited with status: $status"
    read -r -p "Press Enter to close this window..."
' bash "$KAFKA_DIR" &

CONSUMER_TERMINAL_PID=""

for attempt in {1..30}; do
    CONSUMER_TERMINAL_PID="$(find_mintty_pid "Commodity Consumer")"

    if [[ "$CONSUMER_TERMINAL_PID" =~ ^[0-9]+$ ]]; then
        break
    fi

    sleep 1
done

if [[ ! "$CONSUMER_TERMINAL_PID" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Could not identify Consumer terminal PID."
    exit 1
fi

printf '%s\n' "$CONSUMER_TERMINAL_PID" \
    > "$RUNTIME_DIR/consumer-terminal.pid"

echo "Consumer terminal PID: $CONSUMER_TERMINAL_PID"

# Give the consumer time to initialize its Kafka subscriptions
sleep 3

# Launch the producer in another Git Bash window
echo
echo "[4/4] Starting commodity producer..."

mintty --title="Commodity Producer" bash -lc '
    cd "$1" || exit 1

    set -a
    source .env
    set +a

    echo "Starting commodity producer..."
    python -u commodity_producer.py

    status=$?
    echo
    echo "Producer exited with status: $status"
    read -r -p "Press Enter to close this window..."
' bash "$KAFKA_DIR" &

PRODUCER_TERMINAL_PID=""

for attempt in {1..30}; do
    PRODUCER_TERMINAL_PID="$(find_mintty_pid "Commodity Producer")"

    if [[ "$PRODUCER_TERMINAL_PID" =~ ^[0-9]+$ ]]; then
        break
    fi

    sleep 1
done

if [[ ! "$PRODUCER_TERMINAL_PID" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Could not identify Producer terminal PID."
    exit 1
fi

printf '%s\n' "$PRODUCER_TERMINAL_PID" \
    > "$RUNTIME_DIR/producer-terminal.pid"

echo "Producer terminal PID: $PRODUCER_TERMINAL_PID"

echo
echo "=========================================="
echo " PROJECT STARTUP COMMANDS COMPLETED"
echo "=========================================="
echo
echo "Kafka UI:   http://localhost:8081"
echo "Airflow UI: http://localhost:8080"
echo
echo "Consumer and producer have separate terminals."
echo "Airflow may need additional time to finish initializing."
echo "Database command to view produced data"
echo "docker exec -it commodity-kafka-postgres-1 psql -U postgres -d kafka_db"
echo
