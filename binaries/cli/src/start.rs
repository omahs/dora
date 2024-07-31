use std::{net::IpAddr, path::PathBuf};

use communication_layer_request_reply::TcpRequestReplyConnection;
use dora_core::{
    descriptor::Descriptor,
    topics::{ControlRequest, ControlRequestReply},
};
use eyre::{bail, Context};
use uuid::Uuid;

use crate::{attach::attach_dataflow, connect_to_coordinator};

pub fn start(
    dataflow: PathBuf,
    name: Option<String>,
    coordinator_addr: IpAddr,
    coordinator_port: u16,
    attach: bool,
    detach: bool,
    hot_reload: bool,
    log_level: Option<log::LevelFilter>,
) -> Result<Uuid, eyre::ErrReport> {
    let dataflow_descriptor =
        Descriptor::blocking_read(&dataflow).wrap_err("Failed to read yaml dataflow")?;
    let working_dir = dataflow
        .canonicalize()
        .context("failed to canonicalize dataflow path")?
        .parent()
        .ok_or_else(|| eyre::eyre!("dataflow path has no parent dir"))?
        .to_owned();
    if !coordinator_addr.is_loopback() {
        dataflow_descriptor.check_in_daemon(&working_dir, &[], true)?;
    } else {
        dataflow_descriptor
            .check(&working_dir)
            .wrap_err("Could not validate yaml")?;
    }

    let coordinator_socket = (coordinator_addr, coordinator_port).into();
    let mut session = connect_to_coordinator(coordinator_socket)
        .wrap_err("failed to connect to dora coordinator")?;
    let dataflow_id = start_dataflow(
        dataflow_descriptor.clone(),
        name,
        working_dir,
        &mut *session,
    )?;

    let attach = match (attach, detach) {
        (true, true) => eyre::bail!("both `--attach` and `--detach` are given"),
        (true, false) => true,
        (false, true) => false,
        (false, false) => {
            println!("attaching to dataflow (use `--detach` to run in background)");
            true
        }
    };

    if attach {
        attach_dataflow(
            dataflow_descriptor,
            dataflow,
            dataflow_id,
            &mut *session,
            hot_reload,
            coordinator_socket,
            log_level.unwrap_or(log::LevelFilter::Info),
        )?
    }
    Ok(dataflow_id)
}

pub(crate) fn start_dataflow(
    dataflow: Descriptor,
    name: Option<String>,
    local_working_dir: PathBuf,
    session: &mut TcpRequestReplyConnection,
) -> Result<Uuid, eyre::ErrReport> {
    let reply_raw = session
        .request(
            &serde_json::to_vec(&ControlRequest::Start {
                dataflow,
                name,
                local_working_dir,
            })
            .unwrap(),
        )
        .wrap_err("failed to send start dataflow message")?;

    let result: ControlRequestReply =
        serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;
    match result {
        ControlRequestReply::DataflowStarted { uuid } => {
            eprintln!("{uuid}");
            Ok(uuid)
        }
        ControlRequestReply::Error(err) => bail!("{err}"),
        other => bail!("unexpected start dataflow reply: {other:?}"),
    }
}
