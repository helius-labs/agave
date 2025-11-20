use crate::table_batcher::serialize_as_json_string;
use clickhouse::Row;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone)]
pub enum ClientId {
    SolanaLabs,
    JitoLabs,
    Firedancer,
    Agave,
    Paladin,
    // If new variants are added, update From<u16> and TryFrom<ClientId>.
    Unknown(u16),
}

impl ToString for ClientId {
    fn to_string(&self) -> String {
        match self {
            ClientId::SolanaLabs => "solana_labs".to_string(),
            ClientId::JitoLabs => "jito_labs".to_string(),
            ClientId::Firedancer => "firedancer".to_string(),
            ClientId::Agave => "agave".to_string(),
            ClientId::Paladin => "paladin".to_string(),
            ClientId::Unknown(id) => id.to_string(),
        }
    }
}

impl Serialize for ClientId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for ClientId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "solana_labs" => Ok(ClientId::SolanaLabs),
            "jito_labs" => Ok(ClientId::JitoLabs),
            "firedancer" => Ok(ClientId::Firedancer),
            "agave" => Ok(ClientId::Agave),
            "paladin" => Ok(ClientId::Paladin),
            _ => Ok(ClientId::Unknown(s.parse().unwrap())),
        }
    }
}
impl Into<ClientId> for u16 {
    fn into(self) -> ClientId {
        match self {
            0 => ClientId::SolanaLabs,
            1 => ClientId::JitoLabs,
            2 => ClientId::Firedancer,
            3 => ClientId::Agave,
            4 => ClientId::Paladin,
            _ => ClientId::Unknown(self),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidatorMetadataData {
    pub client_id: ClientId,
    pub name: String,
    pub details: String,
    pub website: String,
    pub icon_url: String,
}

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct ValidatorMetadata {
    pub identity: String,
    #[serde(serialize_with = "serialize_as_json_string")]
    pub data: ValidatorMetadataData,
}
