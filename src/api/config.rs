use axum::{Json, extract::State};
use serde_json::{Value, json};

use crate::config::SettingEntry;
use crate::state::AppState;

pub async fn settings(State(state): State<AppState>) -> Json<Vec<SettingEntry>> {
    Json(state.config.describe())
}

pub async fn ui(State(state): State<AppState>) -> Json<Value> {
    let ui_path = state.config.config_path.join("ui.json");
    if let Ok(content) = tokio::fs::read_to_string(&ui_path).await {
        if let Ok(value) = serde_json::from_str::<Value>(&content) {
            return Json(value);
        }
    }

    Json(default_ui_config())
}

pub fn default_ui_config() -> Value {
    json!({
        "name": "VPSiner",
        "eyebrow": "Simply Observed",
        "links": [
            {
                "icon": "Github",
                "label": "GitHub",
                "url": "https://github.com/skvostik/vpsiner"
            }
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_ui_config() {
        let val = default_ui_config();
        assert_eq!(val["name"], "VPSiner");
        assert_eq!(val["eyebrow"], "Simply Observed");
        let links = val.get("links").and_then(|l| l.as_array()).unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["icon"], "Github");
        assert_eq!(links[0]["label"], "GitHub");
        assert_eq!(links[0]["url"], "https://github.com/skvostik/vpsiner");
    }
}
