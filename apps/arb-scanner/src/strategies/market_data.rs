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

#[derive(Debug, Clone, Default)]
pub struct BinaryBook {
    pub yes: Quote,
    pub no: Quote,
    pub last_update_ms: i64,
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

fn update_snapshot_quote(side: &str, levels: Option<&Vec<Value>>, quote: &mut Quote) {
    let Some(levels) = levels else { return; };

    let mut parsed_prices: Vec<f64> = Vec::new();
    for level in levels {
        if let Some(price) = level.get("price").and_then(parse_f64) {
            parsed_prices.push(price);
        }
    }

    if parsed_prices.is_empty() {
        return;
    }

    if side == "bid" {
        if let Some(best_bid) = parsed_prices.into_iter().max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)) {
            quote.best_bid = best_bid;
        }
    } else if let Some(best_ask) = parsed_prices.into_iter().min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)) {
        quote.best_ask = best_ask;
    }
}

fn update_quote_for_token(book: &mut BinaryBook, token: &str, yes_token: &str, no_token: &str, best_bid: f64, best_ask: f64) {
    if token == yes_token {
        if best_bid > 0.0 {
            book.yes.best_bid = best_bid;
        }
        if best_ask > 0.0 {
            book.yes.best_ask = best_ask;
        }
    } else if token == no_token {
        if best_bid > 0.0 {
            book.no.best_bid = best_bid;
        }
        if best_ask > 0.0 {
            book.no.best_ask = best_ask;
        }
    }
}

fn update_quote_for_binding(
    book: &mut BinaryBook,
    binding: &TokenBinding,
    best_bid: f64,
    best_ask: f64,
) {
    if binding.is_yes {
        if best_bid > 0.0 {
            book.yes.best_bid = best_bid;
        }
        if best_ask > 0.0 {
            book.yes.best_ask = best_ask;
        }
    } else {
        if best_bid > 0.0 {
            book.no.best_bid = best_bid;
        }
        if best_ask > 0.0 {
            book.no.best_ask = best_ask;
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

    let mut updated_markets: HashMap<String, bool> = HashMap::new();

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
            update_snapshot_quote(
                "bid",
                entry.get("bids").and_then(|v| v.as_array()),
                &mut snapshot_quote,
            );
            update_snapshot_quote(
                "ask",
                entry.get("asks").and_then(|v| v.as_array()),
                &mut snapshot_quote,
            );

            let book = books
                .entry(binding.market_key.clone())
                .or_insert_with(BinaryBook::default);
            update_quote_for_binding(
                book,
                binding,
                snapshot_quote.best_bid,
                snapshot_quote.best_ask,
            );
            updated_markets.insert(binding.market_key.clone(), true);
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
                .or_insert_with(BinaryBook::default);
            update_quote_for_binding(book, binding, best_bid, best_ask);
            updated_markets.insert(binding.market_key.clone(), true);
        }
    }

    if updated_markets.is_empty() {
        return 0;
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    for market_key in updated_markets.keys() {
        if let Some(book) = books.get_mut(market_key) {
            book.last_update_ms = now_ms;
        }
    }

    updated_markets.len()
}

pub fn update_book_from_market_ws(payload: &str, yes_token: &str, no_token: &str, book: &mut BinaryBook) -> bool {
    let parsed = match serde_json::from_str::<Value>(payload) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let mut updated = false;

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
            update_snapshot_quote("bid", entry.get("bids").and_then(|v| v.as_array()), &mut snapshot_quote);
            update_snapshot_quote("ask", entry.get("asks").and_then(|v| v.as_array()), &mut snapshot_quote);

            update_quote_for_token(
                book,
                token,
                yes_token,
                no_token,
                snapshot_quote.best_bid,
                snapshot_quote.best_ask,
            );

            updated = true;
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

            update_quote_for_token(book, token, yes_token, no_token, best_bid, best_ask);
            updated = true;
        }
    }

    if updated {
        book.last_update_ms = chrono::Utc::now().timestamp_millis();
    }

    updated
}
