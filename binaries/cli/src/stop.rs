use std::{net::IpAddr, time::Duration};

use communication_layer_request_reply::TcpRequestReplyConnection;
use dora_core::topics::{ControlRequest, ControlRequestReply};
use eyre::{bail, Context, Result};
use uuid::Uuid;

use crate::{connect_to_coordinator, handle_dataflow_result, query_running_dataflows};

pub(crate) fn stop_dataflow_interactive(
    grace_duration: Option<Duration>,
    session: &mut TcpRequestReplyConnection,
) -> eyre::Result<()> {
    let list = query_running_dataflows(session).wrap_err("failed to query running dataflows")?;
    let active = list.get_active();
    if active.is_empty() {
        eprintln!("No dataflows are running");
    } else {
        let selection = inquire::Select::new("Choose dataflow to stop:", active).prompt()?;
        stop_dataflow(selection.uuid, grace_duration, session)?;
    }

    Ok(())
}

pub(crate) fn stop_dataflow_by_name(
    name: String,
    grace_duration: Option<Duration>,
    session: &mut TcpRequestReplyConnection,
) -> Result<(), eyre::ErrReport> {
    let reply_raw = session
        .request(
            &serde_json::to_vec(&ControlRequest::StopByName {
                name,
                grace_duration,
            })
            .unwrap(),
        )
        .wrap_err("failed to send dataflow stop_by_name message")?;
    let result: ControlRequestReply =
        serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;
    match result {
        ControlRequestReply::DataflowStopped { uuid, result } => {
            handle_dataflow_result(result, Some(uuid))
        }
        ControlRequestReply::Error(err) => bail!("{err}"),
        other => bail!("unexpected stop dataflow reply: {other:?}"),
    }
}

pub(crate) fn stop_dataflow(
    uuid: Uuid,
    grace_duration: Option<Duration>,
    session: &mut TcpRequestReplyConnection,
) -> Result<(), eyre::ErrReport> {
    let reply_raw = session
        .request(
            &serde_json::to_vec(&ControlRequest::Stop {
                dataflow_uuid: uuid,
                grace_duration,
            })
            .unwrap(),
        )
        .wrap_err("failed to send dataflow stop message")?;
    let result: ControlRequestReply =
        serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;
    match result {
        ControlRequestReply::DataflowStopped { uuid, result } => {
            handle_dataflow_result(result, Some(uuid))
        }
        ControlRequestReply::Error(err) => bail!("{err}"),
        other => bail!("unexpected stop dataflow reply: {other:?}"),
    }
}

pub fn stop(
    uuid: Option<Uuid>,
    name: Option<String>,
    grace_duration: Option<Duration>,
    coordinator_addr: IpAddr,
    coordinator_port: u16,
) -> Result<()> {
    let mut session = connect_to_coordinator((coordinator_addr, coordinator_port).into())
        .wrap_err("could not connect to dora coordinator")?;
    match (uuid, name) {
        (Some(uuid), _) => stop_dataflow(uuid, grace_duration, &mut *session)?,
        (None, Some(name)) => stop_dataflow_by_name(name, grace_duration, &mut *session)?,
        (None, None) => stop_dataflow_interactive(grace_duration, &mut *session)?,
    }
    Ok(())
}
