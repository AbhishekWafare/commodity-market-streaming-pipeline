
#!/usr/bin/env bash

set -euo pipefail

# Resolve project directory.
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$PROJECT_DIR/.runtime"

echo "=========================================="
echo " STOPPING COMMODITY MARKET PIPELINE"
echo "=========================================="

# Check required applications.
for cmd in docker powershell.exe taskkill.exe; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: $cmd is not available."
        exit 1
    fi
done

# Find the actual Windows PID of a Mintty window by its title.
find_mintty_pid() {
    local title="$1"

    MINTTY_TITLE="$title" powershell.exe -NoProfile -Command '
        $needle = "--title=" + $env:MINTTY_TITLE

        $proc = Get-CimInstance Win32_Process |
            Where-Object {
                $_.Name -eq "mintty.exe" -and
                $_.CommandLine -and
                $_.CommandLine.Contains($needle)
            } |
            Select-Object -First 1

        if ($proc) {
            [Console]::WriteLine($proc.ProcessId)
        }
    ' | tr -d '\r\n'
}

# Verify that a PID belongs to the expected Mintty window.
is_mintty_running() {
    local pid="$1"
    local title="$2"

    MINTTY_PID="$pid" MINTTY_TITLE="$title" \
        powershell.exe -NoProfile -Command '
            $targetPid = [int]$env:MINTTY_PID
            $needle = "--title=" + $env:MINTTY_TITLE

            $proc = Get-CimInstance Win32_Process `
                -Filter "ProcessId = $targetPid" `
                -ErrorAction SilentlyContinue

            if (
                $proc -and
                $proc.Name -eq "mintty.exe" -and
                $proc.CommandLine -and
                $proc.CommandLine.Contains($needle)
            ) {
                exit 0
            }

            exit 1
        ' >/dev/null 2>&1
}

# Stop a terminal and its complete process tree.
stop_terminal() {

    local component="$1"
    local title="$2"
    local pid_file="$3"
    local pid=""

    echo
    echo "Stopping Commodity $component..."

    # Read tracked PID if available.
    if [[ -f "$pid_file" ]]; then
        pid="$(tr -d '\r\n' < "$pid_file")"
    fi

    # Validate the saved PID.
    if [[ ! "$pid" =~ ^[0-9]+$ ]] ||
       ! is_mintty_running "$pid" "$title"; then

        echo "Searching for terminal by title..."
        pid="$(find_mintty_pid "$title")"
    fi

    # No matching terminal found.
    if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
        echo "$component terminal is already closed."
        rm -f "$pid_file"
        return 0
    fi

    echo "Found $title (Windows PID: $pid)."
    echo "Requesting graceful shutdown..."

    # Stop the terminal and its child processes.
    MSYS_NO_PATHCONV=1 taskkill.exe \
        /PID "$pid" /T >/dev/null 2>&1 || true

    # Wait up to 10 seconds.
    for attempt in {1..10}; do

        if ! is_mintty_running "$pid" "$title"; then
            echo "$component terminal closed successfully."
            rm -f "$pid_file"
            return 0
        fi

        sleep 1
    done

    # Force termination if graceful shutdown fails.
    echo "Graceful shutdown timed out. Force stopping..."

    MSYS_NO_PATHCONV=1 taskkill.exe \
        /PID "$pid" /T /F >/dev/null 2>&1 || true

    # Verify shutdown.
    for attempt in {1..5}; do

        if ! is_mintty_running "$pid" "$title"; then
            echo "$component terminal closed successfully."
            rm -f "$pid_file"
            return 0
        fi

        sleep 1
    done

    echo "WARNING: Could not confirm $component terminal shutdown."
}

# 1. Stop Dashboard, Producer and Consumer.

stop_terminal \
    "dashboard" \
    "Commodity Dashboard" \
    "$RUNTIME_DIR/dashboard-terminal.pid"

stop_terminal \
    "producer" \
    "Commodity Producer" \
    "$RUNTIME_DIR/producer-terminal.pid"

stop_terminal \
    "consumer" \
    "Commodity Consumer" \
    "$RUNTIME_DIR/consumer-terminal.pid"

# 2. Stop Airflow.
echo
echo "Stopping Airflow..."

(
    cd "$PROJECT_DIR/Airflow"
    docker compose -p commodity-airflow down --timeout 60
)

# 3. Stop Kafka broker before Zookeeper.
echo
echo "Stopping Kafka broker gracefully..."

(
    cd "$PROJECT_DIR/Kafka"

    docker compose -p commodity-kafka stop --timeout 60 kafka

    echo "Kafka broker stopped."

    # Stop remaining Kafka services.
    docker compose -p commodity-kafka down --timeout 60
)

echo
echo "=========================================="
echo " PROJECT STOPPED"
echo "=========================================="
echo "Consumer and producer shutdown attempted."
echo "Airflow and Kafka containers stopped."
echo "Docker volumes have been preserved."
