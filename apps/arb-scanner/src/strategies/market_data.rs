use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Default)]
pub struct Quote {
    pub best_bid: f64,
    pub best_ask: f64,
}

impl Quote {
    pub fn mid(&self) -> f64 {
        if self.best_bid > 0.0 && self.best_ask > 0.0 {
            (self.best_bid + self.best_ask) / 2.0
        } else if self.best_bid > 0.0 {
            self.best_bid
        } else {
            self.best_ask
        }
    }

    pub fn is_valid(&self) -> bool {
        self.best_bid > 0.0 && self.best_ask > 0.0 && self.best_bid <= self.best_ask
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DepthMetrics {
    pub bid_notional_top5: f64,
    pub ask_notional_top5: f64,
    pub bid_size_top5: f64,
    pub ask_size_top5: f64,
    pub bid_levels: usize,
    pub ask_levels: usize,
}

#[derive(Debug, Clone, Default)]
pub struct BinaryBook {
    pub yes: Quote,
    pub no: Quote,
    pub yes_depth: DepthMetrics,
    pub no_depth: DepthMetrics,
    pub last_update_ms: i64,
    pub yes_update_ms: i64,
    pub no_update_ms: i64,
}

#[derive(Debug, Clone)]
pub struct TokenBinding {
    pub market_key: String,
    pub is_yes: bool,
}

fn parse_f64(v: &Value) -> Option<f64> {
    if let Some(s) = v.as_str() {
        return s.parse::<f64>().ok();
    }
    v.as_f64()
}

fn parse_level_size(level: &Value) -> Option<f64> {
    level
        .get("size")
        .or_else(|| level.get("quantity"))
        .or_else(|| level.get("amount"))
        .or_else(|| level.get("shares"))
        .and_then(parse_f64)
}

fn update_snapshot_quote(side: &str, levels: Option<&Vec<Value>>, quote: &mut Quote, depth: &mut DepthMetrics) {
    let Some(levels) = levels else { return; };

    let mut parsed_levels: Vec<(f64, f64)> = Vec::new();
    for level in levels {
        if let Some(price) = level.get("price").and_then(parse_f64) {
            if price.is_finite() && price > 0.0 {
                let size = parse_level_size(level).unwrap_or(0.0).max(0.0);
                parsed_levels.push((price, size));
            }
        }
    }

    if parsed_levels.is_empty() {
        return;
    }

    if side == "bid" {
        if let Some(best_bid) = parsed_levels
            .iter()
            .map(|(price, _)| *price)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        {
            quote.best_bid = best_bid;
        }
        parsed_levels.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let top = parsed_levels.iter().take(5);
        let mut size_sum = 0.0;
        let mut notional_sum = 0.0;
        for (price, size) in top {
            size_sum += *size;
            notional_sum += *price * *size;
        }
        depth.bid_levels = parsed_levels.len();
        depth.bid_size_top5 = size_sum;
        depth.bid_notional_top5 = notional_sum;
    } else {
        if let Some(best_ask) = parsed_levels
            .iter()
            .map(|(price, _)| *price)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        {
            quote.best_ask = best_ask;
        }
        parsed_levels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let top = parsed_levels.iter().take(5);
        let mut size_sum = 0.0;
        let mut notional_sum = 0.0;
        for (price, size) in top {
            size_sum += *size;
            notional_sum += *price * *size;
        }
        depth.ask_levels = parsed_levels.len();
        depth.ask_size_top5 = size_sum;
        depth.ask_notional_top5 = notional_sum;
    }
}

fn update_quote_for_token(
    book: &mut BinaryBook,
    token: &str,
    yes_token: &str,
    no_token: &str,
    best_bid: f64,
    best_ask: f64,
    depth: Option<&DepthMetrics>,
) {
    if token == yes_token {
        if best_bid > 0.0 {
            book.yes.best_bid = best_bid;
        }
        if best_ask > 0.0 {
            book.yes.best_ask = best_ask;
        }
        if let Some(metrics) = depth {
            book.yes_depth = *metrics;
        }
    } else if token == no_token {
        if best_bid > 0.0 {
            book.no.best_bid = best_bid;
        }
        if best_ask > 0.0 {
            book.no.best_ask = best_ask;
        }
        if let Some(metrics) = depth {
            book.no_depth = *metrics;
        }
    }
}

fn update_quote_for_binding(
    book: &mut BinaryBook,
    binding: &TokenBinding,
    best_bid: f64,
    best_ask: f64,
    depth: Option<&DepthMetrics>,
) {
    if binding.is_yes {
        if best_bid > 0.0 {
            book.yes.best_bid = best_bid;
        }
        if best_ask > 0.0 {
            book.yes.best_ask = best_ask;
        }
        if let Some(metrics) = depth {
            book.yes_depth = *metrics;
        }
    } else {
        if best_bid > 0.0 {
            book.no.best_bid = best_bid;
        }
        if best_ask > 0.0 {
            book.no.best_ask = best_ask;
        }
        if let Some(metrics) = depth {
            book.no_depth = *metrics;
        }
    }
}

pub fn update_books_from_market_ws(
    payload: &str,
    token_bindings: &HashMap<String, TokenBinding>,
    books: &mut HashMap<String, BinaryBook>,
) -> usize {
    let parsed = match serde_json::from_str::<Value>(payload) {
        Ok(v) => v,
        Err(_) => return 0,
    };

    let mut updated_markets: HashMap<String, (bool, bool)> = HashMap::new();

    if let Some(arr) = parsed.as_array() {
        for entry in arr {
            let token = entry
                .get("asset_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let Some(binding) = token_bindings.get(token) else {
                continue;
            };

            let mut snapshot_quote = Quote::default();
            let mut snapshot_depth = DepthMetrics::default();
            update_snapshot_quote(
                "bid",
                entry.get("bids").and_then(|v| v.as_array()),
                &mut snapshot_quote,
                &mut snapshot_depth,
            );
            update_snapshot_quote(
                "ask",
                entry.get("asks").and_then(|v| v.as_array()),
                &mut snapshot_quote,
                &mut snapshot_depth,
            );

            let book = books
                .entry(binding.market_key.clone())
                .or_default();
            update_quote_for_binding(
                book,
                binding,
                snapshot_quote.best_bid,
                snapshot_quote.best_ask,
                Some(&snapshot_depth),
            );
            let sides = updated_markets.entry(binding.market_key.clone()).or_insert((false, false));
            if binding.is_yes { sides.0 = true; } else { sides.1 = true; }
        }
    } else if let Some(price_changes) = parsed.get("price_changes").and_then(|v| v.as_array()) {
        for change in price_changes {
            let token = change
                .get("asset_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let Some(binding) = token_bindings.get(token) else {
                continue;
            };

            let best_bid = change.get("best_bid").and_then(parse_f64).unwrap_or(0.0);
            let best_ask = change.get("best_ask").and_then(parse_f64).unwrap_or(0.0);
            let book = books
                .entry(binding.market_key.clone())
                .or_default();
            update_quote_for_binding(book, binding, best_bid, best_ask, None);
            let sides = updated_markets.entry(binding.market_key.clone()).or_insert((false, false));
            if binding.is_yes { sides.0 = true; } else { sides.1 = true; }
        }
    }

    if updated_markets.is_empty() {
        return 0;
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    for (market_key, (yes_upd, no_upd)) in updated_markets.iter() {
        if let Some(book) = books.get_mut(market_key) {
            book.last_update_ms = now_ms;
            if *yes_upd { book.yes_update_ms = now_ms; }
            if *no_upd { book.no_update_ms = now_ms; }
        }
    }

    updated_markets.len()
}

pub fn update_book_from_market_ws(payload: &str, yes_token: &str, no_token: &str, book: &mut BinaryBook) -> bool {
    let parsed = match serde_json::from_str::<Value>(payload) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let mut yes_updated = false;
    let mut no_updated = false;

    if let Some(arr) = parsed.as_array() {
        for entry in arr {
            let token = entry
                .get("asset_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            if token != yes_token && token != no_token {
                continue;
            }

            let mut snapshot_quote = Quote::default();
            let mut snapshot_depth = DepthMetrics::default();
            update_snapshot_quote("bid", entry.get("bids").and_then(|v| v.as_array()), &mut snapshot_quote, &mut snapshot_depth);
            update_snapshot_quote("ask", entry.get("asks").and_then(|v| v.as_array()), &mut snapshot_quote, &mut snapshot_depth);

            update_quote_for_token(
                book,
                token,
                yes_token,
                no_token,
                snapshot_quote.best_bid,
                snapshot_quote.best_ask,
                Some(&snapshot_depth),
            );

            if token == yes_token { yes_updated = true; }
            if token == no_token { no_updated = true; }
        }
    } else if let Some(price_changes) = parsed.get("price_changes").and_then(|v| v.as_array()) {
        for change in price_changes {
            let token = change
                .get("asset_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            if token != yes_token && token != no_token {
                continue;
            }

            let best_bid = change.get("best_bid").and_then(parse_f64).unwrap_or(0.0);
            let best_ask = change.get("best_ask").and_then(parse_f64).unwrap_or(0.0);

            update_quote_for_token(book, token, yes_token, no_token, best_bid, best_ask, None);
            if token == yes_token { yes_updated = true; }
            if token == no_token { no_updated = true; }
        }
    }

    if yes_updated || no_updated {
        let now_ms = chrono::Utc::now().timestamp_millis();
        book.last_update_ms = now_ms;
        if yes_updated { book.yes_update_ms = now_ms; }
        if no_updated { book.no_update_ms = now_ms; }
    }

    yes_updated || no_updated
}

#[cfg(test)]
mod tests {
    use super::{BinaryBook, TokenBinding, update_books_from_market_ws};
    use std::collections::HashMap;

    #[test]
    fn malformed_ws_payload_is_ignored() {
        let mut bindings: HashMap<String, TokenBinding> = HashMap::new();
        bindings.insert(
            "yes-token".to_string(),
            TokenBinding {
                market_key: "market-1".to_string(),
                is_yes: true,
            },
        );
        let mut books: HashMap<String, BinaryBook> = HashMap::new();
        let updated = update_books_from_market_ws("not-json", &bindings, &mut books);
        assert_eq!(updated, 0);
        assert!(books.is_empty());
    }

    #[test]
    fn unknown_tokens_do_not_mutate_books() {
        let mut bindings: HashMap<String, TokenBinding> = HashMap::new();
        bindings.insert(
            "yes-token".to_string(),
            TokenBinding {
                market_key: "market-1".to_string(),
                is_yes: true,
            },
        );
        let mut books: HashMap<String, BinaryBook> = HashMap::new();
        let payload = r#"[{"asset_id":"other-token","bids":[{"price":"0.51"}],"asks":[{"price":"0.53"}]}]"#;
        let updated = update_books_from_market_ws(payload, &bindings, &mut books);
        assert_eq!(updated, 0);
        assert!(books.is_empty());
    }

    #[test]
    fn price_changes_update_bound_sides_only() {
        let mut bindings: HashMap<String, TokenBinding> = HashMap::new();
        bindings.insert(
            "yes-token".to_string(),
            TokenBinding {
                market_key: "market-1".to_string(),
                is_yes: true,
            },
        );
        bindings.insert(
            "no-token".to_string(),
            TokenBinding {
                market_key: "market-1".to_string(),
                is_yes: false,
            },
        );
        let mut books: HashMap<String, BinaryBook> = HashMap::new();
        let payload = r#"{"price_changes":[
            {"asset_id":"yes-token","best_bid":"0.56","best_ask":"0.58"},
            {"asset_id":"no-token","best_bid":"0.41","best_ask":"0.43"}
        ]}"#;
        let updated = update_books_from_market_ws(payload, &bindings, &mut books);
        assert_eq!(updated, 1);

        let book = books.get("market-1").expect("book created");
        assert!(book.yes.best_bid > 0.0);
        assert!(book.yes.best_ask > 0.0);
        assert!(book.no.best_bid > 0.0);
        assert!(book.no.best_ask > 0.0);
    }
}
