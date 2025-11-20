use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions,
        ReplicaTransactionInfoVersions, Result, SlotStatus,
    },
    log::*,
    std::fmt::{Debug, Formatter},
};

pub struct MyTestPlugin;

impl Debug for MyTestPlugin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MyTestPlugin")
    }
}

impl GeyserPlugin for MyTestPlugin {
    fn name(&self) -> &'static str {
        "MyTestPlugin"
    }

    fn on_load(&mut self, _config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");

        info!("✅ MyTestPlugin loaded!");

        Ok(())
    }

    fn on_unload(&mut self) {
        info!("❌ MyTestPlugin unloaded!");
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        if let ReplicaAccountInfoVersions::V0_0_3(account_info) = account {
            info!(
                "🎯 MyTestPlugin: Account updated in slot {} (balance: {} lamports, startup: {})",
                slot, account_info.lamports, is_startup
            );
        }
        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        if let ReplicaTransactionInfoVersions::V0_0_3(tx_info) = transaction {
            info!(
                "🚀 MyTestPlugin: Transaction notified in slot {} (signature: {:?})",
                slot, tx_info.signature
            );
        }

        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: &SlotStatus,
    ) -> Result<()> {
        info!("📊 MyTestPlugin: Slot {} status: {:?}", slot, status);
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = MyTestPlugin;
    let boxed: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(boxed)
}
