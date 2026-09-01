use base64::Engine;

use crate::model::LogCursor;
use crate::model::LogLevel;

pub fn encode_cursor(cursor: &LogCursor) -> Result<String, String> {
    let json = serde_json::to_vec(cursor).map_err(|err| err.to_string())?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json))
}

pub fn decode_cursor(value: &str) -> Result<LogCursor, String> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|err| err.to_string())?;
    serde_json::from_slice(&bytes).map_err(|err| err.to_string())
}

pub fn detect_level(raw: &str) -> Option<LogLevel> {
    let value = raw.to_ascii_lowercase();
    if let Some(level) = detect_pino_numeric_level(&value) {
        return Some(level);
    }
    // Covers plain tracing-style output where the level is the first word (e.g. "WARN edge req=..."),
    // or the second word when the message starts with its own timestamp before the level
    // (e.g. "2026-08-23T14:22:02Z INFO worker: ...").
    let mut words = value.split_whitespace();
    let word1 = words.next().unwrap_or("");
    let word2 = words.next().unwrap_or("");
    let is_bare_word = |target: &str| word1 == target || word2 == target;
    if value.contains("\"level\":\"error\"")
        || value.contains("level=error")
        || value.contains("[error]")
        || value.contains("error:")
        || is_bare_word("error")
        // "fatal" (e.g. Pino's textual level) is treated as an error.
        || value.contains("\"level\":\"fatal\"")
        || value.contains("level=fatal")
        || value.contains("[fatal]")
        || value.contains("fatal:")
        || is_bare_word("fatal")
    {
        return Some(LogLevel::Error);
    }
    if value.contains("\"level\":\"warn\"")
        || value.contains("level=warn")
        || value.contains("[warn]")
        || value.contains("warn:")
        || is_bare_word("warn")
        // "warning" is a common alias for warn-level output.
        || value.contains("\"level\":\"warning\"")
        || value.contains("level=warning")
        || value.contains("[warning]")
        || value.contains("warning:")
        || is_bare_word("warning")
    {
        return Some(LogLevel::Warn);
    }
    if value.contains("\"level\":\"info\"")
        || value.contains("level=info")
        || value.contains("[info]")
        || value.contains("info:")
        || is_bare_word("info")
    {
        return Some(LogLevel::Info);
    }
    if value.contains("\"level\":\"debug\"")
        || value.contains("level=debug")
        || value.contains("[debug]")
        || value.contains("debug:")
        || is_bare_word("debug")
        // "verbose" is a common alias for debug-level output.
        || value.contains("\"level\":\"verbose\"")
        || value.contains("level=verbose")
        || value.contains("[verbose]")
        || value.contains("verbose:")
        || is_bare_word("verbose")
        // "trace" has no dedicated bucket, so it is folded into debug. No bare-word match,
        // since "trace" alone appears too often in unrelated text (e.g. "stack trace").
        || value.contains("\"level\":\"trace\"")
        || value.contains("level=trace")
        || value.contains("[trace]")
    {
        return Some(LogLevel::Debug);
    }
    None
}

/// Matches Pino-style numeric levels, e.g. `"level":30` or `level=50`.
/// Pino scale: 10=trace, 20=debug, 30=info, 40=warn, 50=error, 60=fatal.
fn detect_pino_numeric_level(value: &str) -> Option<LogLevel> {
    for needle in ["\"level\":", "level="] {
        let mut search_start = 0;
        while let Some(found) = value[search_start..].find(needle) {
            let mut start = search_start + found + needle.len();
            search_start = start;
            // Tolerate an optional wrapping quote, e.g. `"level":"30"`.
            if value[start..].starts_with('"') {
                start += 1;
            }
            let digits_end = value[start..]
                .find(|ch: char| !ch.is_ascii_digit())
                .map_or(value.len(), |offset| start + offset);
            if digits_end > start {
                if let Ok(number) = value[start..digits_end].parse::<u32>() {
                    if let Some(level) = pino_level_from_number(number) {
                        return Some(level);
                    }
                }
            }
        }
    }
    None
}

fn pino_level_from_number(number: u32) -> Option<LogLevel> {
    match number {
        10..=29 => Some(LogLevel::Debug),
        30..=39 => Some(LogLevel::Info),
        40..=49 => Some(LogLevel::Warn),
        50..=u32::MAX => Some(LogLevel::Error),
        _ => None,
    }
}

pub fn strip_ansi_escape_codes(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut output = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != 0x1b {
            let character = value[index..].chars().next().expect("valid UTF-8 boundary");
            output.push(character);
            index += character.len_utf8();
            continue;
        }

        index += 1;
        if index >= bytes.len() {
            break;
        }
        match bytes[index] {
            b'[' => {
                index += 1;
                while index < bytes.len() && !(0x40..=0x7e).contains(&bytes[index]) {
                    index += 1;
                }
                index += usize::from(index < bytes.len());
            }
            b']' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == 0x07 {
                        index += 1;
                        break;
                    }
                    if bytes[index] == 0x1b && bytes.get(index + 1) == Some(&b'\\') {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }
    output
}

pub fn week_database_name(ts_ms: i64) -> Option<String> {
    let seconds = ts_ms.div_euclid(1_000);
    let datetime = time::OffsetDateTime::from_unix_timestamp(seconds).ok()?;
    let date = datetime.date();
    let week = date.iso_week();
    let year = match (date.month(), week) {
        (time::Month::December, 1) => date.year() + 1,
        (time::Month::January, 52 | 53) => date.year() - 1,
        _ => date.year(),
    };
    Some(format!("{}_w{:02}.db", year, week))
}

pub fn database_week_start_ms(name: &str) -> Option<i64> {
    let stem = name.strip_suffix(".db")?;
    let (year, week) = stem.split_once("_w")?;
    let year = year.parse().ok()?;
    let week = week.parse().ok()?;
    let date = time::Date::from_iso_week_date(year, week, time::Weekday::Monday).ok()?;
    Some(time::OffsetDateTime::new_utc(date, time::Time::MIDNIGHT).unix_timestamp() * 1_000)
}

pub fn format_timestamp_ms(ts_ms: i64) -> String {
    let Ok(datetime) =
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ts_ms) * 1_000_000)
    else {
        return "invalid timestamp".to_string();
    };
    datetime
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "invalid timestamp".to_string())
}

pub fn sanitize_fts_query(raw: &str) -> Option<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in raw.chars() {
        match ch {
            '"' => {
                if in_quotes {
                    in_quotes = false;
                    if !current.is_empty() {
                        tokens.push(std::mem::take(&mut current));
                    }
                } else {
                    if !current.is_empty() {
                        tokens.push(std::mem::take(&mut current));
                    }
                    in_quotes = true;
                }
            }
            ch if ch.is_whitespace() && !in_quotes => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            ch => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    if tokens.is_empty() {
        return None;
    }

    let query = tokens
        .into_iter()
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR ");

    Some(query)
}

pub fn safe_service_path(service: &str) -> String {
    service
        .split(['/', '\\'])
        .filter(|part| !part.is_empty() && *part != "." && *part != "..")
        .collect::<Vec<_>>()
        .join("_")
}

pub fn parse_docker_timestamp(raw: &str) -> (i64, &str) {
    let Some((prefix, message)) = raw.split_once(' ') else {
        return (0, raw);
    };
    let Ok(datetime) =
        time::OffsetDateTime::parse(prefix, &time::format_description::well_known::Rfc3339)
    else {
        return (0, raw);
    };
    let ts = datetime.unix_timestamp_nanos().div_euclid(1_000_000) as i64;
    (ts, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_common_log_levels() {
        assert_eq!(
            detect_level(r#"{"level":"error","msg":"failed"}"#),
            Some(LogLevel::Error)
        );
        assert_eq!(detect_level("request level=warn"), Some(LogLevel::Warn));
        assert_eq!(detect_level("[INFO] listening"), Some(LogLevel::Info));
        assert_eq!(detect_level("debug: details"), Some(LogLevel::Debug));
        assert_eq!(detect_level("plain message"), None);
        assert_eq!(
            detect_level("WARN edge req=dd6023a6 msg=something happened"),
            Some(LogLevel::Warn)
        );
        assert_eq!(
            detect_level("2026-08-23T14:22:02.350816Z INFO worker: Service says hello:"),
            Some(LogLevel::Info)
        );
        assert_eq!(
            detect_level("[VERBOSE] chatty details"),
            Some(LogLevel::Debug)
        );
    }

    #[test]
    fn names_iso_week_database() {
        assert_eq!(
            week_database_name(1_738_368_000_000),
            Some("2025_w05.db".into())
        );
    }

    #[test]
    fn sanitizes_service_path() {
        assert_eq!(safe_service_path("shop/web"), "shop_web");
        assert_eq!(safe_service_path("../shop"), "shop");
    }

    #[test]
    fn parses_docker_timestamp_prefix() {
        let (ts, message) = parse_docker_timestamp("2025-02-05T12:00:00.000000000Z hello");
        assert_eq!(ts, 1_738_756_800_000);
        assert_eq!(message, "hello");
    }

    #[test]
    fn formats_timestamp_as_rfc3339() {
        assert_eq!(
            format_timestamp_ms(1_738_756_800_123),
            "2025-02-05T12:00:00.123Z"
        );
    }

    #[test]
    fn detects_fatal_as_error() {
        assert_eq!(
            detect_level(r#"{"level":"fatal","msg":"crashed"}"#),
            Some(LogLevel::Error)
        );
        assert_eq!(
            detect_level("level=fatal shutting down"),
            Some(LogLevel::Error)
        );
        assert_eq!(detect_level("[FATAL] out of memory"), Some(LogLevel::Error));
        assert_eq!(
            detect_level("FATAL unrecoverable state"),
            Some(LogLevel::Error)
        );
    }

    #[test]
    fn detects_pino_numeric_levels() {
        assert_eq!(
            detect_level(r#"{"level":30,"msg":"listening"}"#),
            Some(LogLevel::Info)
        );
        assert_eq!(
            detect_level(r#"{"level":60,"msg":"crashed"}"#),
            Some(LogLevel::Error)
        );
        assert_eq!(
            detect_level(r#"{"level":50,"msg":"failed"}"#),
            Some(LogLevel::Error)
        );
        assert_eq!(
            detect_level(r#"{"level":40,"msg":"careful"}"#),
            Some(LogLevel::Warn)
        );
        assert_eq!(
            detect_level(r#"{"level":20,"msg":"details"}"#),
            Some(LogLevel::Debug)
        );
        assert_eq!(
            detect_level(r#"{"level":10,"msg":"trace"}"#),
            Some(LogLevel::Debug)
        );
        assert_eq!(detect_level("level=30 something"), Some(LogLevel::Info));
    }

    #[test]
    fn sanitizes_fts_queries() {
        assert_eq!(
            sanitize_fts_query(r#"q="some=value with spaces" another"#),
            Some(r#""q=" OR "some=value with spaces" OR "another""#.into())
        );
        assert_eq!(
            sanitize_fts_query(r#""some=value with spaces" another"#),
            Some(r#""some=value with spaces" OR "another""#.into())
        );
        assert_eq!(
            sanitize_fts_query(r#""missing trailing quote"#),
            Some(r#""missing trailing quote""#.into())
        );
        assert_eq!(
            sanitize_fts_query("hello   world"),
            Some(r#""hello" OR "world""#.into())
        );
        assert_eq!(
            sanitize_fts_query(r#"timeout OR"#),
            Some(r#""timeout" OR "OR""#.into())
        );
        assert_eq!(
            sanitize_fts_query(r#"foo"bar"#),
            Some(r#""foo" OR "bar""#.into())
        );
        assert_eq!(sanitize_fts_query(""), None);
        assert_eq!(sanitize_fts_query("   "), None);
        assert_eq!(sanitize_fts_query(r#""""#), None);
    }

    #[test]
    fn detects_warning_alias() {
        assert_eq!(
            detect_level(r#"{"level":"warning","msg":"careful"}"#),
            Some(LogLevel::Warn)
        );
        assert_eq!(
            detect_level("level=warning something"),
            Some(LogLevel::Warn)
        );
        assert_eq!(
            detect_level("[WARNING] disk almost full"),
            Some(LogLevel::Warn)
        );
        assert_eq!(detect_level("warning: retrying"), Some(LogLevel::Warn));
        assert_eq!(
            detect_level("WARNING disk almost full"),
            Some(LogLevel::Warn)
        );
    }

    #[test]
    fn does_not_treat_bare_trace_word_as_debug() {
        assert_eq!(detect_level("stack trace follows"), None);
        assert_eq!(
            detect_level("[TRACE] entering function"),
            Some(LogLevel::Debug)
        );
        assert_eq!(
            detect_level(r#"{"level":"trace","msg":"entering"}"#),
            Some(LogLevel::Debug)
        );
    }
}
