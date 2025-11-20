# MyTestPlugin - Geyser Plugin Example

## What This Plugin Does

This is a simple test Geyser plugin that logs:
- ✅ When it loads
- 🎯 Account updates (slot, balance, startup flag)
- 📊 Slot status changes (Processed, Confirmed, Rooted)
- ❌ When it unloads

## Build Instructions

```bash
# From the my_test_plugin directory
cargo build --release
```

**Note:** There's currently a deprecation error in the agave dependencies. To fix:

1. Wait for agave to update the deprecated `NotEnoughAccountKeys` to `MissingAccount`
2. Or build against a stable agave version

## Usage

Once built successfully:

```bash
# Start test-validator with the plugin
cd ..
RUST_LOG=info ./target/release/solana-test-validator \
  --geyser-plugin-config my_test_plugin/plugin-config.json
```

## Expected Output

```
✅ MyTestPlugin loaded!
📊 MyTestPlugin: Slot 1 status: Processed
🎯 MyTestPlugin: Account updated in slot 1 (balance: 1000000000 lamports, startup: false)
📊 MyTestPlugin: Slot 1 status: Confirmed
📊 MyTestPlugin: Slot 1 status: Rooted
```

## Complete Flow Test

1. Start validator with plugin (see above)
2. In another terminal, send a transaction:
   ```bash
   ./target/release/solana transfer <recipient> 1 --url http://localhost:8899
   ```
3. Watch the logs for the complete flow:
   - 🚀 BROADCAST (shreds sent)
   - 📥 RECEIVED (shreds received)
   - ✅ STORING (shreds stored in blockstore)
   - 🔔 GEYSER NOTIFY (account update notification)
   - 📤 GEYSER PLUGIN CALL (your plugin called)
   - 🎯 MyTestPlugin (your plugin processes update)
   - ✅ GEYSER PLUGIN SUCCESS (plugin completes)

## Files

- `src/lib.rs` - Plugin implementation
- `Cargo.toml` - Plugin dependencies
- `plugin-config.json` - Configuration for test-validator
- `README.md` - This file
