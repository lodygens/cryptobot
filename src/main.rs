use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{fs, time::Duration};
use teloxide::prelude::*;
use tokio::time;

#[derive(Debug, Deserialize)]
struct Config {
    pairs: Vec<PairConfig>,
    telegram: TelegramConfig,
}

#[derive(Debug, Deserialize)]
struct PairConfig {
    pair: String,
    interval: String,
}

#[derive(Debug, Deserialize)]
struct TelegramConfig {
    bot_token: String,
    chat_id: String,
}

#[derive(Debug, Deserialize)]
struct KrakenResponse {
    result: serde_json::Value,
    error: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Read config
    let config_str = fs::read_to_string("config.yaml")
        .context("Failed to read config.yaml")?;
    let config: Config = serde_yaml::from_str(&config_str)
        .context("Failed to parse config.yaml")?;

    // Initialize Telegram bot
    let bot = Bot::new(&config.telegram.bot_token);
    let chat_id = config.telegram.chat_id.parse::<i64>()?;

    // Create HTTP client
    let client = reqwest::Client::new();

    loop {
        for pair_config in &config.pairs {
            let url = format!(
                "https://api.kraken.com/0/public/Ticker?pair={}",
                pair_config.pair
            );

            match client.get(&url).send().await {
                Ok(response) => {
                    match response.json::<KrakenResponse>().await {
                        Ok(kraken_data) => {
                            if !kraken_data.error.is_empty() {
                                eprintln!("Kraken API error: {:?}", kraken_data.error);
                                continue;
                            }

                            // Extract price from the response
                            let price = kraken_data.result
                                .as_object()
                                .and_then(|obj| obj.values().next())
                                .and_then(|pair| pair.get("c"))
                                .and_then(|c| c.get(0))
                                .and_then(|p| p.as_str())
                                .unwrap_or("N/A");

                            // Format message
                            let message = format!(
                                "🔔 Price Update\n\nPair: {}\nPrice: ${}\nTime: {}",
                                pair_config.pair,
                                price,
                                Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
                            );

                            // Send to Telegram
                            if let Err(e) = bot.send_message(ChatId(chat_id), message).await {
                                eprintln!("Failed to send Telegram message: {}", e);
                            }
                        }
                        Err(e) => eprintln!("Failed to parse Kraken response: {}", e),
                    }
                }
                Err(e) => eprintln!("Failed to fetch from Kraken: {}", e),
            }
        }

        // Wait for the specified interval
        time::sleep(Duration::from_secs(3600)).await; // Default 1h interval
    }
}

