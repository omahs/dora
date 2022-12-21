use crate::tcp_utils::tcp_send;

use self::runtime::spawn_runtime_node;
use dora_core::{
    config::{format_duration, CommunicationConfig, NodeId},
    daemon_messages::{DaemonCoordinatorEvent, SpawnDataflowNodes, SpawnNodeParams},
    descriptor::{self, collect_dora_timers, CoreNodeKind, Descriptor},
};
use eyre::{bail, eyre, ContextCompat, WrapErr};
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::{BTreeMap, HashMap},
    env::consts::EXE_EXTENSION,
    path::Path,
};
use tokio::net::TcpStream;
use tokio_stream::wrappers::IntervalStream;
use uuid::Uuid;

mod runtime;

pub async fn run_dataflow(
    dataflow_path: &Path,
    runtime: &Path,
    daemon_connections: &mut HashMap<String, TcpStream>,
) -> eyre::Result<()> {
    let tasks = spawn_dataflow(runtime, dataflow_path, daemon_connections)
        .await?
        .tasks;
    await_tasks(tasks).await
}

pub async fn spawn_dataflow(
    runtime: &Path,
    dataflow_path: &Path,
    daemon_connections: &mut HashMap<String, TcpStream>,
) -> eyre::Result<SpawnedDataflow> {
    let mut runtime = runtime.with_extension(EXE_EXTENSION);
    let descriptor = read_descriptor(dataflow_path).await.wrap_err_with(|| {
        format!(
            "failed to read dataflow descriptor at {}",
            dataflow_path.display()
        )
    })?;
    let working_dir = dataflow_path
        .canonicalize()
        .context("failed to canoncialize dataflow path")?
        .parent()
        .ok_or_else(|| eyre!("canonicalized dataflow path has no parent"))?
        .to_owned();
    let nodes = descriptor.resolve_aliases();
    let dora_timers = collect_dora_timers(&nodes);
    let uuid = Uuid::new_v4();
    let communication_config = {
        let mut config = descriptor.communication;
        // add uuid as prefix to ensure isolation
        config.add_topic_prefix(&uuid.to_string());
        config
    };
    if nodes
        .iter()
        .any(|n| matches!(n.kind, CoreNodeKind::Runtime(_)))
    {
        match which::which(runtime.as_os_str()) {
            Ok(path) => {
                runtime = path;
            }
            Err(err) => {
                let err = eyre!(err).wrap_err(format!(
                    "There is no runtime at {}, or it is not a file",
                    runtime.display()
                ));
                bail!("{err:?}")
            }
        }
    }

    let mut custom_nodes = BTreeMap::new();
    for node in nodes {
        match node.kind {
            CoreNodeKind::Runtime(_) => todo!(),
            CoreNodeKind::Custom(n) => {
                custom_nodes.insert(
                    node.id.clone(),
                    SpawnNodeParams {
                        node_id: node.id,
                        node: n,
                        working_dir: working_dir.clone(),
                    },
                );
            }
        }
    }

    let spawn_command = SpawnDataflowNodes {
        dataflow_id: uuid,
        nodes: custom_nodes,
    };
    let message = serde_json::to_vec(&DaemonCoordinatorEvent::Spawn(spawn_command))?;
    let daemon_connection = daemon_connections
        .get_mut("")
        .wrap_err("no daemon connection")?; // TODO: take from dataflow spec
    tcp_send(daemon_connection, &message)
        .await
        .wrap_err("failed to send spawn message to daemon")?;

    Ok(SpawnedDataflow {
        tasks: FuturesUnordered::new(), // TODO
        communication_config,
        uuid,
    })
}

pub struct SpawnedDataflow {
    pub uuid: Uuid,
    pub communication_config: CommunicationConfig,
    pub tasks: FuturesUnordered<tokio::task::JoinHandle<Result<(), eyre::ErrReport>>>,
}

pub async fn await_tasks(
    mut tasks: FuturesUnordered<tokio::task::JoinHandle<Result<(), eyre::ErrReport>>>,
) -> eyre::Result<()> {
    while let Some(task_result) = tasks.next().await {
        task_result
            .wrap_err("failed to join async task")?
            .wrap_err("custom node failed")?;
    }
    Ok(())
}

async fn read_descriptor(file: &Path) -> Result<Descriptor, eyre::Error> {
    let descriptor_file = tokio::fs::read(file)
        .await
        .context("failed to open given file")?;
    let descriptor: Descriptor =
        serde_yaml::from_slice(&descriptor_file).context("failed to parse given descriptor")?;
    Ok(descriptor)
}

fn command_init_common_env(
    command: &mut tokio::process::Command,
    node_id: &NodeId,
    communication: &dora_core::config::CommunicationConfig,
) -> Result<(), eyre::Error> {
    command.env(
        "DORA_NODE_ID",
        serde_yaml::to_string(&node_id).wrap_err("failed to serialize custom node ID")?,
    );
    command.env(
        "DORA_COMMUNICATION_CONFIG",
        serde_yaml::to_string(communication)
            .wrap_err("failed to serialize communication config")?,
    );
    Ok(())
}
