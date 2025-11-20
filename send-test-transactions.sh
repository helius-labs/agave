#!/bin/bash

# Script to continuously send test transactions to the validator
# Usage: ./send-test-transactions.sh [interval_seconds]

INTERVAL=${1:-0.1}  # Default 100ms between transactions
RPC_URL="http://localhost:8899"

echo "Starting continuous transaction sender..."
echo "Interval: ${INTERVAL} seconds"
echo "RPC URL: ${RPC_URL}"
echo ""

# Counter for transactions sent
COUNT=0

while true; do
    COUNT=$((COUNT + 1))

    # Generate a random recipient to ensure unique transactions
    TEMP_KEY=$(mktemp)
    ./target/release/solana-keygen new --no-bip39-passphrase --silent --force --outfile "$TEMP_KEY" > /dev/null 2>&1
    RECIPIENT=$(./target/release/solana-keygen pubkey "$TEMP_KEY" 2>/dev/null)
    rm -f "$TEMP_KEY"

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sending transaction #${COUNT} to ${RECIPIENT}..."

    # Send transaction
    ./target/release/solana \
        --keypair test-ledger/faucet-keypair.json \
        transfer ${RECIPIENT} 0.001 \
        --url http://localhost:8899 \
        --allow-unfunded-recipient \
        --skip-preflight 2>&1 | grep -E 'Signature:|Error:' &

    # Wait for interval
    sleep ${INTERVAL}
done
