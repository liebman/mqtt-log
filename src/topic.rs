//! MQTT topic filter validation, matching and filename sanitisation.
//!
//! Pure functions so they can be exhaustively unit tested.

/// Validates an MQTT 3.1.1/5 topic filter (spec section 4.7).
pub fn validate_filter(filter: &str) -> Result<(), String> {
    if filter.is_empty() {
        return Err("topic filter must not be empty".to_string());
    }
    if filter.len() > 65_535 {
        return Err("topic filter must not exceed 65535 bytes".to_string());
    }
    if filter.contains('\0') {
        return Err("topic filter must not contain a NUL character".to_string());
    }

    let levels: Vec<&str> = filter.split('/').collect();
    let last = levels.len() - 1;
    for (index, level) in levels.iter().enumerate() {
        if level.contains('#') {
            if *level != "#" {
                return Err(format!("`#` must occupy an entire level in `{filter}`"));
            }
            if index != last {
                return Err(format!("`#` must be the last level in `{filter}`"));
            }
        }
        if level.contains('+') && *level != "+" {
            return Err(format!("`+` must occupy an entire level in `{filter}`"));
        }
    }
    Ok(())
}

/// Does a concrete `topic` match the (already validated) `filter`?
///
/// Mirrors the broker's matching rules, including the `$`-prefix exception:
/// a filter starting with `#` or `+` never matches `$SYS/...` style topics.
pub fn matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('#') || filter.starts_with('+')) {
        return false;
    }

    let mut filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/');
    loop {
        match (filter_levels.next(), topic_levels.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => continue,
            (Some(f), Some(t)) if f == t => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// Turns a topic into a single, safe path component.
///
/// `/` becomes `_` so `sensors/kitchen/temp` cannot escape the output
/// directory, and characters that are awkward or illegal on common
/// filesystems (including the wildcards themselves) become `_`.
pub fn sanitize(topic: &str) -> String {
    let mut out = String::with_capacity(topic.len());
    for ch in topic.chars() {
        let safe = match ch {
            '/' | '\\' | '+' | '#' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        };
        out.push(safe);
    }

    if out.is_empty() || out == "." || out == ".." {
        out.insert(0, '_');
    }

    // Most filesystems cap a single component at 255 bytes.
    if out.len() > 200 {
        let mut cut = 200;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out.truncate(cut);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_well_formed_filters() {
        for filter in [
            "#",
            "+",
            "a/b",
            "a/+/c",
            "a/#",
            "+/+",
            "sensors/kitchen/temp",
        ] {
            assert!(validate_filter(filter).is_ok(), "{filter} should be valid");
        }
    }

    #[test]
    fn rejects_malformed_filters() {
        for filter in ["", "a/#/b", "a#", "a/b+", "a/+b", "##"] {
            assert!(
                validate_filter(filter).is_err(),
                "{filter} should be rejected"
            );
        }
    }

    #[test]
    fn rejects_nul() {
        assert!(validate_filter("a\0b").is_err());
    }

    #[test]
    fn matching_obeys_wildcards() {
        assert!(matches("app/#", "app/one"));
        assert!(matches("app/#", "app/one/two"));
        assert!(matches("app/#", "app"));
        assert!(!matches("app/#", "other/one"));
        assert!(matches("sensors/+/log", "sensors/kitchen/log"));
        assert!(!matches("sensors/+/log", "sensors/kitchen/sub/log"));
        assert!(matches("a/b", "a/b"));
        assert!(!matches("a/b", "a/c"));
    }

    #[test]
    fn dollar_topics_are_not_matched_by_leading_wildcards() {
        assert!(!matches("#", "$SYS/broker/uptime"));
        assert!(!matches("+/broker/uptime", "$SYS/broker/uptime"));
        assert!(matches("$SYS/#", "$SYS/broker/uptime"));
    }

    #[test]
    fn sanitise_makes_one_path_component() {
        assert_eq!(sanitize("app/one/two"), "app_one_two");
        assert_eq!(sanitize("a#b+c"), "a_b_c");
        assert_eq!(sanitize(".."), "_..");
        assert_eq!(sanitize(""), "_");
        assert!(!sanitize("a/b").contains('/'));
    }

    #[test]
    fn sanitise_caps_length_on_a_char_boundary() {
        let long = "é".repeat(300);
        let out = sanitize(&long);
        assert!(out.len() <= 200, "{} bytes", out.len());
        assert!(out.chars().all(|c| c == 'é'));
    }
}
