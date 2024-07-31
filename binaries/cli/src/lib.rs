use communication_layer_request_reply::{RequestReplyLayer, TcpLayer, TcpRequestReplyConnection};
use dora_coordinator::Event;
use dora_core::{
    descriptor::Descriptor,
    topics::{
        ControlRequest, ControlRequestReply, DataflowList, DORA_COORDINATOR_PORT_CONTROL_DEFAULT,
        DORA_COORDINATOR_PORT_DEFAULT, DORA_DAEMON_LOCAL_LISTEN_PORT_DEFAULT,
    },
};
use dora_daemon::Daemon;
use duration_str::parse;
use eyre::{bail, Context};
use formatting::FormatDataflowError;
use start::start;
use std::{io::Write, net::SocketAddr};
use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    time::Duration,
};
use stop::{stop_dataflow, stop_dataflow_by_name, stop_dataflow_interactive};
use tabwriter::TabWriter;
use tokio::runtime::Builder;
use uuid::Uuid;

mod attach;
pub mod build;
mod check;
mod formatting;
mod graph;
mod logs;
pub mod start;
pub mod stop;
mod template;
pub mod up;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
const LISTEN_WILDCARD: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

#[derive(Debug, clap::Parser)]
#[clap(version)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

/// dora-rs cli client
#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Check if the coordinator and the daemon is running.
    Check {
        /// Path to the dataflow descriptor file (enables additional checks)
        #[clap(long, value_name = "PATH", value_hint = clap::ValueHint::FilePath)]
        dataflow: Option<PathBuf>,
        /// Address of the dora coordinator
        #[clap(long, value_name = "IP", default_value_t = LOCALHOST)]
        coordinator_addr: IpAddr,
        /// Port number of the coordinator control server
        #[clap(long, value_name = "PORT", default_value_t = DORA_COORDINATOR_PORT_CONTROL_DEFAULT)]
        coordinator_port: u16,
    },
    /// Generate a visualization of the given graph using mermaid.js. Use --open to open browser.
    Graph {
        /// Path to the dataflow descriptor file
        #[clap(value_name = "PATH", value_hint = clap::ValueHint::FilePath)]
        dataflow: PathBuf,
        /// Visualize the dataflow as a Mermaid diagram (instead of HTML)
        #[clap(long, action)]
        mermaid: bool,
        /// Open the HTML visualization in the browser
        #[clap(long, action)]
        open: bool,
    },
    /// Run build commands provided in the given dataflow.
    Build {
        /// Path to the dataflow descriptor file
        #[clap(value_name = "PATH", value_hint = clap::ValueHint::FilePath)]
        dataflow: PathBuf,
    },
    /// Generate a new project or node. Choose the language between Rust, Python, C or C++.
    New {
        #[clap(flatten)]
        args: CommandNew,
        #[clap(hide = true, long)]
        internal_create_with_path_dependencies: bool,
    },
    /// Spawn coordinator and daemon in local mode (with default config)
    Up {
        /// Use a custom configuration
        #[clap(long, hide = true, value_name = "PATH", value_hint = clap::ValueHint::FilePath)]
        config: Option<PathBuf>,
    },
    /// Destroy running coordinator and daemon. If some dataflows are still running, they will be stopped first.
    Destroy {
        /// Use a custom configuration
        #[clap(long, hide = true)]
        config: Option<PathBuf>,
        /// Address of the dora coordinator
        #[clap(long, value_name = "IP", default_value_t = LOCALHOST)]
        coordinator_addr: IpAddr,
        /// Port number of the coordinator control server
        #[clap(long, value_name = "PORT", default_value_t = DORA_COORDINATOR_PORT_CONTROL_DEFAULT)]
        coordinator_port: u16,
    },
    /// Start the given dataflow path. Attach a name to the running dataflow by using --name.
    Start {
        /// Path to the dataflow descriptor file
        #[clap(value_name = "PATH", value_hint = clap::ValueHint::FilePath)]
        dataflow: PathBuf,
        /// Assign a name to the dataflow
        #[clap(long)]
        name: Option<String>,
        /// Address of the dora coordinator
        #[clap(long, value_name = "IP", default_value_t = LOCALHOST)]
        coordinator_addr: IpAddr,
        /// Port number of the coordinator control server
        #[clap(long, value_name = "PORT", default_value_t = DORA_COORDINATOR_PORT_CONTROL_DEFAULT)]
        coordinator_port: u16,
        /// Attach to the dataflow and wait for its completion
        #[clap(long, action)]
        attach: bool,
        /// Run the dataflow in background
        #[clap(long, action)]
        detach: bool,
        /// Enable hot reloading (Python only)
        #[clap(long, action)]
        hot_reload: bool,
    },
    /// Stop the given dataflow UUID. If no id is provided, you will be able to choose between the running dataflows.
    Stop {
        /// UUID of the dataflow that should be stopped
        uuid: Option<Uuid>,
        /// Name of the dataflow that should be stopped
        #[clap(long)]
        name: Option<String>,
        /// Kill the dataflow if it doesn't stop after the given duration
        #[clap(long, value_name = "DURATION")]
        #[arg(value_parser = parse)]
        grace_duration: Option<Duration>,
        /// Address of the dora coordinator
        #[clap(long, value_name = "IP", default_value_t = LOCALHOST)]
        coordinator_addr: IpAddr,
        /// Port number of the coordinator control server
        #[clap(long, value_name = "PORT", default_value_t = DORA_COORDINATOR_PORT_CONTROL_DEFAULT)]
        coordinator_port: u16,
    },
    /// List running dataflows.
    List {
        /// Address of the dora coordinator
        #[clap(long, value_name = "IP", default_value_t = LOCALHOST)]
        coordinator_addr: IpAddr,
        /// Port number of the coordinator control server
        #[clap(long, value_name = "PORT", default_value_t = DORA_COORDINATOR_PORT_CONTROL_DEFAULT)]
        coordinator_port: u16,
    },
    // Planned for future releases:
    // Dashboard,
    /// Show logs of a given dataflow and node.
    #[command(allow_missing_positional = true)]
    Logs {
        /// Identifier of the dataflow
        #[clap(value_name = "UUID_OR_NAME")]
        dataflow: Option<String>,
        /// Show logs for the given node
        #[clap(value_name = "NAME")]
        node: String,
        /// Address of the dora coordinator
        #[clap(long, value_name = "IP", default_value_t = LOCALHOST)]
        coordinator_addr: IpAddr,
        /// Port number of the coordinator control server
        #[clap(long, value_name = "PORT", default_value_t = DORA_COORDINATOR_PORT_CONTROL_DEFAULT)]
        coordinator_port: u16,
    },
    // Metrics,
    // Stats,
    // Get,
    // Upgrade,
    /// Run daemon
    Daemon {
        /// Unique identifier for the machine (required for distributed dataflows)
        #[clap(long)]
        machine_id: Option<String>,
        /// The inter daemon IP address and port this daemon will bind to.
        #[clap(long, default_value_t = SocketAddr::new(LISTEN_WILDCARD, 0))]
        inter_daemon_addr: SocketAddr,
        /// Local listen port for event such as dynamic node.
        #[clap(long, default_value_t = DORA_DAEMON_LOCAL_LISTEN_PORT_DEFAULT)]
        local_listen_port: u16,
        /// Address and port number of the dora coordinator
        #[clap(long, default_value_t = SocketAddr::new(LOCALHOST, DORA_COORDINATOR_PORT_DEFAULT))]
        coordinator_addr: SocketAddr,
        #[clap(long, hide = true)]
        run_dataflow: Option<PathBuf>,
        /// Suppresses all log output to stdout.
        #[clap(long)]
        quiet: bool,
    },
    /// Run runtime
    Runtime,
    /// Run coordinator
    Coordinator {
        /// Network interface to bind to for daemon communication
        #[clap(long, default_value_t = LISTEN_WILDCARD)]
        interface: IpAddr,
        /// Port number to bind to for daemon communication
        #[clap(long, default_value_t = DORA_COORDINATOR_PORT_DEFAULT)]
        port: u16,
        /// Network interface to bind to for control communication
        #[clap(long, default_value_t = LISTEN_WILDCARD)]
        control_interface: IpAddr,
        /// Port number to bind to for control communication
        #[clap(long, default_value_t = DORA_COORDINATOR_PORT_CONTROL_DEFAULT)]
        control_port: u16,
        /// Suppresses all log output to stdout.
        #[clap(long)]
        quiet: bool,
    },
}

#[derive(Debug, clap::Args)]
pub struct CommandNew {
    /// The entity that should be created
    #[clap(long, value_enum, default_value_t = Kind::Dataflow)]
    kind: Kind,
    /// The programming language that should be used
    #[clap(long, value_enum, default_value_t = Lang::Rust)]
    lang: Lang,
    /// Desired name of the entity
    name: String,
    /// Where to create the entity
    #[clap(hide = true)]
    path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Kind {
    Dataflow,
    CustomNode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Lang {
    Rust,
    Python,
    C,
    Cxx,
}

pub fn run(command: Command) -> eyre::Result<()> {
    let log_level = env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .build()
        .filter();

    match command {
        Command::Check {
            dataflow,
            coordinator_addr,
            coordinator_port,
        } => match dataflow {
            Some(dataflow) => {
                let working_dir = dataflow
                    .canonicalize()
                    .context("failed to canonicalize dataflow path")?
                    .parent()
                    .ok_or_else(|| eyre::eyre!("dataflow path has no parent dir"))?
                    .to_owned();
                Descriptor::blocking_read(&dataflow)?.check(&working_dir)?;
                check::check_environment((coordinator_addr, coordinator_port).into())?
            }
            None => check::check_environment((coordinator_addr, coordinator_port).into())?,
        },
        Command::Graph {
            dataflow,
            mermaid,
            open,
        } => {
            graph::create(dataflow, mermaid, open)?;
        }
        Command::Build { dataflow } => {
            build::build(&dataflow)?;
        }
        Command::New {
            args,
            internal_create_with_path_dependencies,
        } => template::create(args, internal_create_with_path_dependencies)?,
        Command::Up { config } => {
            up::up(config.as_deref())?;
        }
        Command::Logs {
            dataflow,
            node,
            coordinator_addr,
            coordinator_port,
        } => {
            let mut session = connect_to_coordinator((coordinator_addr, coordinator_port).into())
                .wrap_err("failed to connect to dora coordinator")?;
            let list = query_running_dataflows(&mut *session)
                .wrap_err("failed to query running dataflows")?;
            if let Some(dataflow) = dataflow {
                let uuid = Uuid::parse_str(&dataflow).ok();
                let name = if uuid.is_some() { None } else { Some(dataflow) };
                logs::logs(&mut *session, uuid, name, node)?
            } else {
                let active = list.get_active();
                let uuid = match &active[..] {
                    [] => bail!("No dataflows are running"),
                    [uuid] => uuid.clone(),
                    _ => inquire::Select::new("Choose dataflow to show logs:", active).prompt()?,
                };
                logs::logs(&mut *session, Some(uuid.uuid), None, node)?
            }
        }
        Command::Start {
            dataflow,
            name,
            coordinator_addr,
            coordinator_port,
            attach,
            detach,
            hot_reload,
        } => {
            let _uuid = start(
                dataflow,
                name,
                coordinator_addr,
                coordinator_port,
                attach,
                detach,
                hot_reload,
                Some(log_level),
            )?;
        }
        Command::List {
            coordinator_addr,
            coordinator_port,
        } => match connect_to_coordinator((coordinator_addr, coordinator_port).into()) {
            Ok(mut session) => list(&mut *session)?,
            Err(_) => {
                bail!("No dora coordinator seems to be running.");
            }
        },
        Command::Stop {
            uuid,
            name,
            grace_duration,
            coordinator_addr,
            coordinator_port,
        } => {
            let mut session = connect_to_coordinator((coordinator_addr, coordinator_port).into())
                .wrap_err("could not connect to dora coordinator")?;
            match (uuid, name) {
                (Some(uuid), _) => stop_dataflow(uuid, grace_duration, &mut *session)?,
                (None, Some(name)) => stop_dataflow_by_name(name, grace_duration, &mut *session)?,
                (None, None) => stop_dataflow_interactive(grace_duration, &mut *session)?,
            }
        }
        Command::Destroy {
            config,
            coordinator_addr,
            coordinator_port,
        } => up::destroy(
            config.as_deref(),
            (coordinator_addr, coordinator_port).into(),
        )?,
        Command::Coordinator {
            interface,
            port,
            control_interface,
            control_port,
            quiet,
        } => {
            let rt = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("tokio runtime failed")?;
            rt.block_on(async {
                let bind = SocketAddr::new(interface, port);
                let bind_control = SocketAddr::new(control_interface, control_port);
                let (port, task) =
                    dora_coordinator::start(bind, bind_control, futures::stream::empty::<Event>())
                        .await?;
                if !quiet {
                    println!("Listening for incoming daemon connection on {port}");
                }
                task.await
            })
            .context("failed to run dora-coordinator")?
        }
        Command::Daemon {
            coordinator_addr,
            inter_daemon_addr,
            local_listen_port,
            machine_id,
            run_dataflow,
            quiet: _,
        } => {
            let rt = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("tokio runtime failed")?;
            rt.block_on(async {
                match run_dataflow {
                    Some(dataflow_path) => {
                        tracing::info!("Starting dataflow `{}`", dataflow_path.display());
                        if coordinator_addr != SocketAddr::new(LOCALHOST, DORA_COORDINATOR_PORT_DEFAULT){
                            tracing::info!(
                                "Not using coordinator addr {} as `run_dataflow` is for local dataflow only. Please use the `start` command for remote coordinator",
                                coordinator_addr
                            );
                        }

                        let result = Daemon::run_dataflow(&dataflow_path).await?;
                        handle_dataflow_result(result, None)
                    }
                    None => {
                        if coordinator_addr.ip() == LOCALHOST {
                            tracing::info!("Starting in local mode");
                        }
                        Daemon::run(coordinator_addr, machine_id.unwrap_or_default(), inter_daemon_addr, local_listen_port).await
                    }
                }
            })
            .context("failed to run dora-daemon")?
        }
        Command::Runtime => dora_runtime::main().context("Failed to run dora-runtime")?,
    };

    Ok(())
}

fn handle_dataflow_result(
    result: dora_core::topics::DataflowResult,
    uuid: Option<Uuid>,
) -> Result<(), eyre::Error> {
    if result.is_ok() {
        Ok(())
    } else {
        Err(match uuid {
            Some(uuid) => {
                eyre::eyre!("Dataflow {uuid} failed:\n{}", FormatDataflowError(&result))
            }
            None => {
                eyre::eyre!("Dataflow failed:\n{}", FormatDataflowError(&result))
            }
        })
    }
}
fn list(session: &mut TcpRequestReplyConnection) -> Result<(), eyre::ErrReport> {
    let list = query_running_dataflows(session)?;

    let mut tw = TabWriter::new(vec![]);
    tw.write_all(b"UUID\tName\tStatus\n")?;
    for entry in list.0 {
        let uuid = entry.id.uuid;
        let name = entry.id.name.unwrap_or_default();
        let status = match entry.status {
            dora_core::topics::DataflowStatus::Running => "Running",
            dora_core::topics::DataflowStatus::Finished => "Succeeded",
            dora_core::topics::DataflowStatus::Failed => "Failed",
        };
        tw.write_all(format!("{uuid}\t{name}\t{status}\n").as_bytes())?;
    }
    tw.flush()?;
    let formatted = String::from_utf8(tw.into_inner()?)?;

    println!("{formatted}");

    Ok(())
}

fn query_running_dataflows(session: &mut TcpRequestReplyConnection) -> eyre::Result<DataflowList> {
    let reply_raw = session
        .request(&serde_json::to_vec(&ControlRequest::List).unwrap())
        .wrap_err("failed to send list message")?;
    let reply: ControlRequestReply =
        serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;
    let ids = match reply {
        ControlRequestReply::DataflowList(list) => list,
        ControlRequestReply::Error(err) => bail!("{err}"),
        other => bail!("unexpected list dataflow reply: {other:?}"),
    };

    Ok(ids)
}

fn connect_to_coordinator(
    coordinator_addr: SocketAddr,
) -> std::io::Result<Box<TcpRequestReplyConnection>> {
    TcpLayer::new().connect(coordinator_addr)
}
