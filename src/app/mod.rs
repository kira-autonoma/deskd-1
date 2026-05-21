//! Application layer — wiring, commands, and runtime modules.
//!
//! Groups internal modules that aren't part of the public library API
//! (domain types and port traits). Reduces lib.rs fan-out.

pub mod a2a;
pub mod a2a_jwt;
pub mod a2a_server;
pub mod acp;
pub mod adapters;
pub mod agent;
pub mod agent_components;
pub mod agent_process;
pub mod agent_registry;
pub mod agent_state;
pub mod alerts;
pub mod bus;
pub mod bus_api;
pub mod cli;
pub mod commands;
pub mod config_changeset;
pub mod config_reload;
pub mod config_watcher;
pub mod context;
pub mod context_log;
pub mod context_size;
pub mod doctor;
pub mod graph;
pub mod jsonrpc;
pub mod mcp;
pub mod mcp_protocol;
pub mod mcp_service;
pub mod mcp_tools;
pub mod message;
pub mod metrics;
pub mod process_builder;
pub mod reload_state;
pub mod schedule;
pub mod scope;
pub mod serve;
pub mod statemachine;
pub mod task;
pub mod tasklog;
pub mod timeout_sweep;
pub mod tmux_launcher;
pub mod unified_inbox;
pub mod worker;
pub mod workflow;
