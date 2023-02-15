use std::{
    fmt::Display,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};
use uuid::Uuid;

pub const DORA_COORDINATOR_PORT_DEFAULT: u16 = 0xD02A;

pub const MANUAL_STOP: &str = "dora/stop";

pub fn control_socket_addr() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 6012)
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum ControlRequest {
    Start {
        dataflow_path: PathBuf,
        name: Option<String>,
    },
    Stop {
        dataflow_uuid: Uuid,
        grace_period: Option<Duration>,
    },
    StopByName {
        name: String,
        grace_period: Option<Duration>,
    },
    Destroy,
    List,
    DaemonConnected,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StartDataflowResult {
    Ok { uuid: Uuid },
    Error(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StopDataflowResult {
    Ok,
    Error(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ListDataflowResult {
    Ok { dataflows: Vec<DataflowId> },
    Error(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataflowId {
    pub uuid: Uuid,
    pub name: Option<String>,
}

impl Display for DataflowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "[{name}] {}", self.uuid)
        } else {
            write!(f, "[<unnamed>] {}", self.uuid)
        }
    }
}
