use crate::cli::ConfigAction;
use crate::error::{CliError, CliResult};
use std::path::Path;

const CONFIG_FILE: &str = "config.toml";

pub fn run(action: ConfigAction, fluree_dir: &Path) -> CliResult<()> {
    let config_path = fluree_dir.join(CONFIG_FILE);

    match action {
        ConfigAction::Get { key } => {
            let content = std::fs::read_to_string(&config_path).unwrap_or_default();
            let doc: toml::Value = content.parse().map_err(|e: toml::de::Error| {
                CliError::Config(format!("failed to parse config: {e}"))
            })?;

            match lookup_key(&doc, &key) {
                Some(val) => {
                    println!("{}", format_toml_value(&val));
                    Ok(())
                }
                None => Err(CliError::NotFound(format!("config key '{key}' not set"))),
            }
        }

        ConfigAction::Set { key, value } => {
            let content = std::fs::read_to_string(&config_path).unwrap_or_default();
            let mut doc: toml_edit::DocumentMut =
                content.parse().map_err(|e: toml_edit::TomlError| {
                    CliError::Config(format!("failed to parse config: {e}"))
                })?;

            set_key(&mut doc, &key, &value)?;

            std::fs::write(&config_path, doc.to_string())
                .map_err(|e| CliError::Config(format!("failed to write config: {e}")))?;

            println!("Set '{key}' = '{value}'");
            Ok(())
        }

        ConfigAction::List => {
            let content = std::fs::read_to_string(&config_path).unwrap_or_default();
            if content.trim().is_empty() {
                println!("(no configuration set)");
                return Ok(());
            }

            let doc: toml::Value = content.parse().map_err(|e: toml::de::Error| {
                CliError::Config(format!("failed to parse config: {e}"))
            })?;

            print_toml_flat("", &doc);
            Ok(())
        }
    }
}

/// Look up a dotted key path in a TOML value.
fn lookup_key<'a>(val: &'a toml::Value, key: &str) -> Option<&'a toml::Value> {
    let parts: Vec<&str> = key.split('.').collect();
    let mut current = val;
    for part in &parts {
        current = current.get(part)?;
    }
    Some(current)
}

/// Set a dotted key path in a toml_edit document.
fn set_key(doc: &mut toml_edit::DocumentMut, key: &str, value: &str) -> CliResult<()> {
    let parts: Vec<&str> = key.split('.').collect();

    if parts.is_empty() {
        return Err(CliError::Usage("empty config key".into()));
    }

    // Navigate to the parent table, creating intermediate tables as needed
    let mut table = doc.as_table_mut();
    for part in &parts[..parts.len() - 1] {
        if !table.contains_key(part) {
            table.insert(part, toml_edit::Item::Table(toml_edit::Table::new()));
        }
        table = table
            .get_mut(part)
            .and_then(|item| item.as_table_mut())
            .ok_or_else(|| CliError::Config(format!("key component '{part}' is not a table")))?;
    }

    let leaf = parts.last().unwrap();

    // Auto-detect value type
    let toml_value = if value == "true" {
        toml_edit::value(true)
    } else if value == "false" {
        toml_edit::value(false)
    } else if let Ok(n) = value.parse::<i64>() {
        toml_edit::value(n)
    } else if let Ok(f) = value.parse::<f64>() {
        toml_edit::value(f)
    } else {
        toml_edit::value(value)
    };

    table.insert(leaf, toml_value);
    Ok(())
}

/// Print a TOML value in flat key=value format.
fn print_toml_flat(prefix: &str, val: &toml::Value) {
    match val {
        toml::Value::Table(map) => {
            for (k, v) in map {
                let full_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                print_toml_flat(&full_key, v);
            }
        }
        _ => {
            println!("{prefix} = {}", format_toml_value(val));
        }
    }
}

/// Format a TOML value for display.
fn format_toml_value(val: &toml::Value) -> String {
    match val {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(n) => n.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_toml_value).collect();
            format!("[{}]", items.join(", "))
        }
        toml::Value::Table(_) => {
            // For tables, show as TOML inline
            val.to_string()
        }
        toml::Value::Datetime(dt) => dt.to_string(),
    }
}
