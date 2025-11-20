use std::{collections::HashMap, time::Duration};

use serde::Deserialize;

const DEFAULT_FLUSH_BATCH_SIZE: usize = 500_000;
const DEFAULT_FLUSH_INTERVAL: Duration = std::time::Duration::from_millis(500);

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseConfig {
    pub url: String,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_flush_batch_size")]
    pub flush_batch_size: usize,
    #[serde(default = "default_flush_interval")]
    pub flush_interval: Duration,
    #[serde(default)]
    pub env: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

impl ClickhouseConfig {
    pub fn get_tags(&self) -> HashMap<String, String> {
        let mut tags = self.tags.clone();
        if tags.get("env").is_none() {
            tags.insert(
                "env".to_string(),
                self.env.clone().unwrap_or("unknown_env".to_string()),
            );
        }
        if tags.get("host").is_none() {
            tags.insert(
                "host".to_string(),
                self.host.clone().unwrap_or("unknown_host".to_string()),
            );
        }
        tags
    }
}

fn default_user() -> String {
    "default".to_string()
}

fn default_database() -> String {
    "default".to_string()
}

fn default_flush_batch_size() -> usize {
    DEFAULT_FLUSH_BATCH_SIZE
}

fn default_flush_interval() -> Duration {
    DEFAULT_FLUSH_INTERVAL
}
