#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: $0 <px_port> <command> [args...]"
    exit 1
fi

# Check if px_port is valid then start process exporter
if ! [[ "$1" =~ ^[0-9]+$ ]] || [ $1 -lt 2 ] || [ $1 -gt 65535 ]; then
    echo "No valid port specified for process-exporter, skipping"
else
    echo "Starting process-exporter on port $1"
    prometheus-process-exporter \
        -config.path=/data/cfoverlay/process_exporter.yml \
        -web.listen-address=":${1}" &
fi

# Store the command and arguments
COMMAND="${@:2}"

# Flag to control the restart loop
SHOULD_EXIT=false

# Signal handler function
cleanup() {
    echo
    echo "[$(date)] Received signal, stopping auto-restart loop"
    SHOULD_EXIT=true
    # Kill the child process if it's running
    if [ ! -z "$CHILD_PID" ]; then
        kill $CHILD_PID 2>/dev/null
    fi
}

# Set up signal traps
trap cleanup SIGINT SIGTERM

echo "Starting auto-restart wrapper for: $COMMAND"
echo

while true; do
    if [ "$SHOULD_EXIT" = true ]; then
        echo "Exiting auto-restart loop"
        break
    fi
    
    echo "[$(date)] Starting: $COMMAND"
    
    # Execute the command in background to capture PID
    $COMMAND &
    CHILD_PID=$!
    
    # Wait for the command to complete
    wait $CHILD_PID
    EXIT_CODE=$?
    unset CHILD_PID
    
    echo "[$(date)] Process exited with code: $EXIT_CODE"
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Process completed successfully, exiting auto-restart loop"
        break
    fi
    
    echo "Non-zero exit code. Restarting in 5 seconds"
    for i in {1..5}; do
        if [ "$SHOULD_EXIT" = true ]; then
            break
        fi
        sleep 1
    done
done