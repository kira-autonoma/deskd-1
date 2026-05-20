//! A2A (Agent-to-Agent) protocol support.
//!
//! Phase 1: Agent Card generation from workspace + user configs.
//! Generates `/.well-known/agent-card.json` per the A2A spec.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::config::{UserConfig, WorkspaceConfig};

/// A2A Agent Card — describes an agent's capabilities for discovery.
/// See: https://a2a-protocol.org/latest/topics/agent-discovery/
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    /// Human-readable agent name.
    pub name: String,
    /// Description of what this agent/instance does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Public URL for this agent (A2A endpoint base).
    pub url: String,
    /// A2A protocol version.
    pub version: String,
    /// Supported capabilities.
    pub capabilities: AgentCapabilities,
    /// Skills this agent can perform.
    pub skills: Vec<AgentSkill>,
    /// Needs — what this agent wants done (custom extension to A2A spec).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<AgentNeed>,
    /// Authentication schemes accepted.
    pub authentication: AgentAuthentication,
}

/// Capabilities advertised in the Agent Card.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    pub streaming: bool,
    pub push_notifications: bool,
}

/// A skill the agent can perform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// A need the agent wants fulfilled (custom extension).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentNeed {
    pub id: String,
    pub description: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub priority: String,
}

/// Authentication schemes supported by this agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAuthentication {
    pub schemes: Vec<String>,
    /// JWKS key set for JWT verification. Present when auth scheme is "jwt".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwks: Option<crate::app::a2a_jwt::Jwks>,
}

/// Build an Agent Card from workspace config + all user configs.
///
/// Each agent's skills come from its deskd.yaml `skills:` section.
/// The workspace-level `a2a:` block provides the URL, auth, and description.
pub fn build_agent_card(workspace: &WorkspaceConfig) -> Result<AgentCard> {
    let a2a = workspace
        .a2a
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("workspace.yaml has no `a2a:` section"))?;

    // Collect skills and needs from each agent's deskd.yaml.
    let mut skills = Vec::new();
    let mut needs = Vec::new();
    for agent_def in &workspace.agents {
        let config_path = agent_def.config_path();
        let user_cfg = match UserConfig::load(&config_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    agent = agent_def.name,
                    path = config_path,
                    "skipping agent config: {e}"
                );
                continue;
            }
        };
        for skill in &user_cfg.skills {
            skills.push(AgentSkill {
                id: format!("{}/{}", agent_def.name, skill.id),
                name: skill.name.clone(),
                description: skill.description.clone(),
                tags: skill.tags.clone(),
            });
        }
        for need in &user_cfg.needs {
            needs.push(AgentNeed {
                id: format!("{}/{}", agent_def.name, need.id),
                description: need.description.clone(),
                tags: need.tags.clone(),
                priority: need.priority.clone(),
            });
        }
    }

    let auth_schemes = match a2a.auth.as_str() {
        "jwt" => vec!["jwt".to_string()],
        "none" => vec![],
        _ => {
            if a2a.api_key.is_some() {
                vec!["apiKey".to_string()]
            } else {
                vec![]
            }
        }
    };

    let name = a2a
        .description
        .as_deref()
        .unwrap_or("deskd instance")
        .to_string();

    Ok(AgentCard {
        name,
        description: a2a.description.clone(),
        url: a2a.url.clone(),
        version: "0.1.0".to_string(),
        capabilities: AgentCapabilities {
            streaming: true,
            push_notifications: false,
        },
        skills,
        needs,
        authentication: AgentAuthentication {
            schemes: auth_schemes,
            jwks: None,
        },
    })
}

/// Build an Agent Card from workspace config + explicitly provided user configs.
/// Used when user configs are already loaded (e.g., in tests or when configs
/// are in non-standard locations).
pub fn build_agent_card_with_configs(
    workspace: &WorkspaceConfig,
    agent_configs: &[(&str, &UserConfig)],
) -> Result<AgentCard> {
    let a2a = workspace
        .a2a
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("workspace.yaml has no `a2a:` section"))?;

    let mut skills = Vec::new();
    let mut needs = Vec::new();
    for (agent_name, user_cfg) in agent_configs {
        for skill in &user_cfg.skills {
            skills.push(AgentSkill {
                id: format!("{}/{}", agent_name, skill.id),
                name: skill.name.clone(),
                description: skill.description.clone(),
                tags: skill.tags.clone(),
            });
        }
        for need in &user_cfg.needs {
            needs.push(AgentNeed {
                id: format!("{}/{}", agent_name, need.id),
                description: need.description.clone(),
                tags: need.tags.clone(),
                priority: need.priority.clone(),
            });
        }
    }

    let auth_schemes = match a2a.auth.as_str() {
        "jwt" => vec!["jwt".to_string()],
        "none" => vec![],
        _ => {
            if a2a.api_key.is_some() {
                vec!["apiKey".to_string()]
            } else {
                vec![]
            }
        }
    };

    let name = a2a
        .description
        .as_deref()
        .unwrap_or("deskd instance")
        .to_string();

    Ok(AgentCard {
        name,
        description: a2a.description.clone(),
        url: a2a.url.clone(),
        version: "0.1.0".to_string(),
        capabilities: AgentCapabilities {
            streaming: true,
            push_notifications: false,
        },
        skills,
        needs,
        authentication: AgentAuthentication {
            schemes: auth_schemes,
            jwks: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{A2aConfig, AgentDef, SkillDef};
    use std::collections::HashMap;

    fn make_workspace(agents: Vec<AgentDef>, a2a: Option<A2aConfig>) -> WorkspaceConfig {
        WorkspaceConfig {
            agents,
            containers: HashMap::new(),
            rooms: vec![],
            admin_telegram_ids: vec![],
            a2a,
            alerts: None,
            web: None,
            federation: None,
            github_webhooks: None,
            metrics: None,
            cost: None,
        }
    }

    fn make_a2a_config() -> A2aConfig {
        A2aConfig {
            url: "https://dev.agent.example.com".into(),
            api_key: Some("test-key".into()),
            listen: "0.0.0.0:3000".into(),
            description: Some("Dev workspace".into()),
            auth: "api_key".into(),
            private_key: None,
            trusted_keys: vec![],
        }
    }

    fn make_user_config_with_needs(
        skills: Vec<SkillDef>,
        needs: Vec<crate::config::NeedDef>,
    ) -> UserConfig {
        UserConfig {
            skills,
            needs,
            ..Default::default()
        }
    }

    fn make_user_config(skills: Vec<SkillDef>) -> UserConfig {
        UserConfig {
            skills,
            ..Default::default()
        }
    }

    #[test]
    fn agent_card_with_skills() {
        let workspace = make_workspace(vec![], Some(make_a2a_config()));
        let user_cfg = make_user_config(vec![
            SkillDef {
                id: "code-review".into(),
                name: "Code Review".into(),
                description: "Review PRs and check architecture".into(),
                tags: vec!["go".into(), "rust".into()],
            },
            SkillDef {
                id: "implement".into(),
                name: "Implementation".into(),
                description: "Implement features from specs".into(),
                tags: vec!["go".into()],
            },
        ]);

        let card = build_agent_card_with_configs(&workspace, &[("dev", &user_cfg)]).unwrap();

        assert_eq!(card.name, "Dev workspace");
        assert_eq!(card.url, "https://dev.agent.example.com");
        assert_eq!(card.skills.len(), 2);
        assert_eq!(card.skills[0].id, "dev/code-review");
        assert_eq!(card.skills[0].name, "Code Review");
        assert_eq!(card.skills[0].tags, vec!["go", "rust"]);
        assert_eq!(card.skills[1].id, "dev/implement");
        assert!(card.capabilities.streaming);
        assert!(!card.capabilities.push_notifications);
        assert_eq!(card.authentication.schemes, vec!["apiKey"]);
    }

    #[test]
    fn agent_card_no_a2a_config_errors() {
        let workspace = make_workspace(vec![], None);
        let result = build_agent_card_with_configs(&workspace, &[]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no `a2a:` section")
        );
    }

    #[test]
    fn agent_card_no_api_key_empty_auth() {
        let mut a2a = make_a2a_config();
        a2a.api_key = None;
        let workspace = make_workspace(vec![], Some(a2a));
        let card = build_agent_card_with_configs(&workspace, &[]).unwrap();
        assert!(card.authentication.schemes.is_empty());
    }

    #[test]
    fn agent_card_multiple_agents() {
        let workspace = make_workspace(vec![], Some(make_a2a_config()));
        let cfg1 = make_user_config(vec![SkillDef {
            id: "review".into(),
            name: "Review".into(),
            description: "".into(),
            tags: vec![],
        }]);
        let cfg2 = make_user_config(vec![SkillDef {
            id: "lint".into(),
            name: "Lint".into(),
            description: "Run linting".into(),
            tags: vec!["go".into()],
        }]);

        let card =
            build_agent_card_with_configs(&workspace, &[("collab", &cfg1), ("archlint", &cfg2)])
                .unwrap();

        assert_eq!(card.skills.len(), 2);
        assert_eq!(card.skills[0].id, "collab/review");
        assert_eq!(card.skills[1].id, "archlint/lint");
    }

    #[test]
    fn agent_card_serializes_to_json() {
        let workspace = make_workspace(vec![], Some(make_a2a_config()));
        let user_cfg = make_user_config(vec![SkillDef {
            id: "test".into(),
            name: "Test".into(),
            description: "Run tests".into(),
            tags: vec!["rust".into()],
        }]);

        let card = build_agent_card_with_configs(&workspace, &[("dev", &user_cfg)]).unwrap();
        let json = serde_json::to_string_pretty(&card).unwrap();

        // Verify camelCase serialization
        assert!(json.contains("\"pushNotifications\""));
        assert!(json.contains("\"streaming\""));
        assert!(!json.contains("push_notifications")); // should be camelCase
    }

    #[test]
    fn agent_card_default_name_when_no_description() {
        let mut a2a = make_a2a_config();
        a2a.description = None;
        let workspace = make_workspace(vec![], Some(a2a));
        let card = build_agent_card_with_configs(&workspace, &[]).unwrap();
        assert_eq!(card.name, "deskd instance");
        assert!(card.description.is_none());
    }

    #[test]
    fn agent_card_with_needs() {
        let workspace = make_workspace(vec![], Some(make_a2a_config()));
        let user_cfg = make_user_config_with_needs(
            vec![],
            vec![
                crate::config::NeedDef {
                    id: "want-restart".into(),
                    description: "Restart agent from Telegram".into(),
                    tags: vec!["ux".into(), "telegram".into()],
                    priority: "high".into(),
                },
                crate::config::NeedDef {
                    id: "want-dashboard".into(),
                    description: "Web dashboard for monitoring".into(),
                    tags: vec!["ui".into()],
                    priority: "medium".into(),
                },
            ],
        );

        let card = build_agent_card_with_configs(&workspace, &[("kira", &user_cfg)]).unwrap();
        assert_eq!(card.needs.len(), 2);
        assert_eq!(card.needs[0].id, "kira/want-restart");
        assert_eq!(card.needs[0].priority, "high");
        assert_eq!(card.needs[1].id, "kira/want-dashboard");
        assert!(card.skills.is_empty());
    }

    #[test]
    fn agent_card_needs_omitted_when_empty() {
        let workspace = make_workspace(vec![], Some(make_a2a_config()));
        let user_cfg = make_user_config(vec![]);
        let card = build_agent_card_with_configs(&workspace, &[("dev", &user_cfg)]).unwrap();
        assert!(card.needs.is_empty());
        let json = serde_json::to_string(&card).unwrap();
        assert!(
            !json.contains("\"needs\""),
            "empty needs should be omitted from JSON"
        );
    }
}
