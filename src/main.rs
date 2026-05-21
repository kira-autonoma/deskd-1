use anyhow::bail;
use clap::Parser;

use deskd::app::cli::{Cli, Commands};
use deskd::app::{commands, mcp, serve};
use deskd::config;

/// Resolve workspace config path: explicit flag > serve state > error.
fn resolve_workspace_config(explicit: Option<String>) -> anyhow::Result<String> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(state) = config::ServeState::load() {
        return Ok(state.workspace_config);
    }
    bail!("no --config provided and deskd serve is not running (no serve state found)")
}

/// Resolve agent config (deskd.yaml) path: explicit flag > serve state > error.
fn resolve_agent_config(explicit: Option<String>) -> anyhow::Result<String> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(state) = config::ServeState::load()
        && let Some(agent) = state.find_agent_config()
    {
        return Ok(agent.config_path.clone());
    }
    bail!("no --config provided and deskd serve is not running (no serve state found)")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
                .unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve {
            config: config_path,
        } => {
            serve::serve(config_path).await?;
        }
        Commands::Mcp { agent } => {
            mcp::run(&agent).await?;
        }
        Commands::McpChannel { agent } => {
            mcp::run_channel(&agent).await?;
        }
        Commands::Agent { action } => {
            commands::agent::handle(action).await?;
        }
        Commands::A2a { action } => {
            let config_path = match &action {
                deskd::app::cli::A2aAction::AgentCard { config }
                | deskd::app::cli::A2aAction::Serve { config, .. } => config.clone(),
                deskd::app::cli::A2aAction::Keygen {} => None,
            };
            let config_path = resolve_workspace_config(config_path)?;
            commands::a2a::handle(action, &config_path).await?;
        }
        Commands::Bus { action } => {
            commands::bus::handle(action).await?;
        }
        Commands::Graph { action } => {
            commands::graph::handle(action).await?;
        }
        Commands::Sm {
            config: config_opt,
            action,
        } => {
            let config_path = resolve_agent_config(config_opt)?;
            let user_cfg = config::UserConfig::load(&config_path)?;
            commands::sm::handle(action, &user_cfg, &config_path).await?;
        }
        Commands::Task { action } => {
            commands::task::handle(action)?;
        }
        Commands::Schedule {
            action,
            config: config_opt,
        } => {
            let config_path = resolve_agent_config(config_opt)?;
            commands::schedule::handle(action, &config_path)?;
        }
        Commands::Remind {
            name,
            r#in: duration_str,
            at,
            target,
            message,
        } => {
            commands::remind::handle(name, duration_str, at, target, message)?;
        }
        Commands::Tui { socket } => {
            commands::tui::handle(socket)?;
        }
        Commands::Status {
            config: config_opt,
            format,
        } => {
            let config_path = resolve_workspace_config(config_opt)?;
            commands::status::handle(&config_path, &format).await?;
        }
        Commands::Restart { config } => {
            commands::restart::handle(config).await?;
        }
        Commands::Reload {
            config,
            agent,
            socket,
        } => {
            commands::reload::handle(config, agent, socket).await?;
        }
        Commands::Upgrade { install_dir } => {
            commands::upgrade::handle(install_dir).await?;
        }
        Commands::Usage {
            period,
            agent,
            format,
        } => {
            commands::usage::run(&period, agent.as_deref(), &format).await?;
        }
        Commands::Context { format } => {
            commands::context::run(&format).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use deskd::app::commands::{parse_duration_secs, remind};
    use deskd::config;

    #[test]
    fn test_handle_remind_writes_file() {
        // remind::handle now resolves work_dir from DESKD_AGENT_CONFIG (parent
        // dir) to match the rest of the system (#467). Use a tempdir as that
        // parent and write a placeholder deskd.yaml so the resolution works.
        // env mutation is serialised because setenv is not thread-safe.
        let _env_guard = deskd::test_support::env_lock().blocking_lock();
        let tmp = std::path::PathBuf::from(format!(
            "/tmp/deskd-test-remind-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let cfg_path = tmp.join("deskd.yaml");
        std::fs::write(&cfg_path, "model: claude-sonnet-4-6\n").unwrap();

        let prev_cfg = std::env::var("DESKD_AGENT_CONFIG").ok();
        unsafe {
            std::env::set_var("DESKD_AGENT_CONFIG", &cfg_path);
        }

        remind::handle(
            "kira".to_string(),
            Some("30m".to_string()),
            None,
            None,
            "test reminder".to_string(),
        )
        .unwrap();

        let remind_dir = tmp.join(".deskd").join("reminders");
        let files: Vec<_> = std::fs::read_dir(&remind_dir)
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("json"))
            .collect();

        assert_eq!(files.len(), 1, "expected exactly one reminder file");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let parsed: config::RemindDef = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed.target, "agent:kira");
        assert_eq!(parsed.message, "test reminder");

        // Restore env so other tests in this binary aren't affected.
        unsafe {
            match prev_cfg {
                Some(v) => std::env::set_var("DESKD_AGENT_CONFIG", v),
                None => std::env::remove_var("DESKD_AGENT_CONFIG"),
            }
        }
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_schedule_list_parses_config() {
        let tmp = std::env::temp_dir().join(format!(
            "deskd-test-sched-list-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let cfg_path = tmp.join("deskd.yaml");
        std::fs::write(
            &cfg_path,
            r#"
model: claude-sonnet-4-6
schedules:
  - cron: "0 */5 * * * *"
    target: "agent:dev"
    action: github_poll
    config:
      repos:
        - kgatilin/deskd
  - cron: "0 0 9 * * *"
    target: "agent:dev"
    action: raw
    config: "Morning brief"
"#,
        )
        .unwrap();

        let user_cfg = config::UserConfig::load(cfg_path.to_str().unwrap()).unwrap();
        assert_eq!(user_cfg.schedules.len(), 2);
        assert_eq!(user_cfg.schedules[0].cron, "0 */5 * * * *");
        assert_eq!(user_cfg.schedules[0].target, "agent:dev");
        assert_eq!(user_cfg.schedules[1].cron, "0 0 9 * * *");

        use deskd::app::cli::ScheduleSubcommand;
        deskd::app::commands::schedule::handle(
            ScheduleSubcommand::List,
            cfg_path.to_str().unwrap(),
        )
        .unwrap();

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_schedule_add_rm_roundtrip() {
        let tmp = std::env::temp_dir().join(format!(
            "deskd-test-sched-addm-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let cfg_path = tmp.join("deskd.yaml");
        std::fs::write(&cfg_path, "model: claude-sonnet-4-6\nschedules: []\n").unwrap();

        let path_str = cfg_path.to_str().unwrap();

        use deskd::app::cli::ScheduleSubcommand;

        deskd::app::commands::schedule::handle(
            ScheduleSubcommand::Add {
                cron: "0 0 9 * * *".to_string(),
                action: "raw".to_string(),
                target: "agent:dev".to_string(),
                config_json: None,
            },
            path_str,
        )
        .unwrap();

        let cfg = config::UserConfig::load(path_str).unwrap();
        assert_eq!(cfg.schedules.len(), 1);
        assert_eq!(cfg.schedules[0].cron, "0 0 9 * * *");
        assert_eq!(cfg.schedules[0].target, "agent:dev");

        deskd::app::commands::schedule::handle(
            ScheduleSubcommand::Add {
                cron: "0 */5 * * * *".to_string(),
                action: "github_poll".to_string(),
                target: "agent:collab".to_string(),
                config_json: Some(r#"{"repos":["kgatilin/deskd"]}"#.to_string()),
            },
            path_str,
        )
        .unwrap();

        let cfg = config::UserConfig::load(path_str).unwrap();
        assert_eq!(cfg.schedules.len(), 2);

        deskd::app::commands::schedule::handle(ScheduleSubcommand::Rm { index: 0 }, path_str)
            .unwrap();

        let cfg = config::UserConfig::load(path_str).unwrap();
        assert_eq!(cfg.schedules.len(), 1);
        assert_eq!(cfg.schedules[0].cron, "0 */5 * * * *");

        assert!(
            deskd::app::commands::schedule::handle(ScheduleSubcommand::Rm { index: 5 }, path_str)
                .is_err()
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_parse_duration_basic() {
        assert_eq!(parse_duration_secs("90s").unwrap(), 90);
        assert_eq!(parse_duration_secs("30m").unwrap(), 30 * 60);
        assert_eq!(parse_duration_secs("1h").unwrap(), 3600);
        assert_eq!(parse_duration_secs("2h30m").unwrap(), 2 * 3600 + 30 * 60);
        assert!(parse_duration_secs("").is_err());
        assert!(parse_duration_secs("30").is_err());
    }
}
