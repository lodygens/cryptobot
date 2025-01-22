use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use redis::Commands;
use serde::{Deserialize, Serialize};
use std::{fs, time::Duration};
use teloxide::prelude::*;
use tokio::time;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Replay mode: resend all stored prices from Redis to Telegram
    #[arg(long)]
    replay: bool,
}

#[derive(Debug, Deserialize)]
struct Config {
    pairs: Vec<PairConfig>,
    telegram: TelegramConfig,
    redis: RedisConfig,
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
struct RedisConfig {
    url: String,
    database: u8,
}

#[derive(Debug, Deserialize)]
struct KrakenResponse {
    result: serde_json::Value,
    error: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PriceData {
    price: String,
    timestamp: String,
}

/*
async fn replay_data(bot: Bot, chat_id: i64, redis_con: &mut redis::Connection, pairs: &[PairConfig]) -> Result<()> {
    for pair_config in pairs {
        let key = format!("history:{}", pair_config.pair);
        let data: Vec<String> = redis_con.lrange(&key, 0, -1)?;
        
        // Convert all entries to PriceData and sort by timestamp
        let mut price_entries: Vec<(String, PriceData)> = data
            .into_iter()
            .filter_map(|json| {
                serde_json::from_str::<PriceData>(&json)
                    .ok()
                    .map(|data| (pair_config.pair.clone(), data))
            })
            .collect();

        price_entries.sort_by(|a, b| a.1.timestamp.cmp(&b.1.timestamp));

        // Send each entry to Telegram
        for (pair, price_data) in price_entries {
            let message = format!(
                "🔄 Historical Price\n\nPair: {}\nPrice: ${}\nTime: {}",
                pair,
                price_data.price,
                price_data.timestamp
            );

            bot.send_message(ChatId(chat_id), message)
                .await
                .context("Failed to send Telegram message")?;

            // Add a small delay to avoid hitting Telegram rate limits
            time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}
*/

async fn replay_data(bot: Bot, chat_id: i64, redis_con: &mut redis::Connection, pairs: &[PairConfig]) -> Result<()> {
    const CHUNK_SIZE: isize = 100; // Process 100 entries at a time
    
    for pair_config in pairs {
        let key = format!("history:{}", pair_config.pair);
        let mut start: isize = 0;
        
        loop {
            // Get a chunk of data
            let data: Vec<String> = redis_con.lrange(&key, start, start + CHUNK_SIZE - 1)?;
            
            // If no more data, break the loop
            if data.is_empty() {
                break;
            }
            
            // Process this chunk
            for json in data {
                if let Ok(price_data) = serde_json::from_str::<PriceData>(&json) {
                    let message = format!(
                        "🔄 Historical Price\n\nPair: {}\nPrice: ${}\nTime: {}",
                        pair_config.pair,
                        price_data.price,
                        price_data.timestamp
                    );

                    bot.send_message(ChatId(chat_id), message)
                        .await
                        .context("Failed to send Telegram message")?;

                    // Add a small delay to avoid hitting Telegram rate limits
                    time::sleep(Duration::from_millis(100)).await;
                }
            }
            
            // Move to next chunk
            start += CHUNK_SIZE;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Read config
    let config_str = fs::read_to_string("config.yaml")
        .context("Failed to read config.yaml")?;
    let config: Config = serde_yaml::from_str(&config_str)
        .context("Failed to parse config.yaml")?;

    // Initialize Redis client
    let client = redis::Client::open(config.redis.url)?;
    let mut con = client.get_connection()?;
    
    // Select the specified database
    let _: () = redis::cmd("SELECT")
        .arg(config.redis.database)
        .query(&mut con)?;

    // Initialize Telegram bot
    let bot = Bot::new(&config.telegram.bot_token);
    let chat_id = config.telegram.chat_id.parse::<i64>()?;

    if args.replay {
        // Replay mode: read from Redis and send to Telegram
        replay_data(bot, chat_id, &mut con, &config.pairs).await?;
        return Ok(());
    }

    // Normal mode: monitor prices
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

                            let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

                            // Store data in Redis
                            let price_data = PriceData {
                                price: price.to_string(),
                                timestamp: timestamp.clone(),
                            };
                            
                            let json = serde_json::to_string(&price_data)?;
                            let _: () = con.set(&pair_config.pair, &json)?;
                            
                            // Also store in a time series (last 24 hours)
                            let key = format!("history:{}", pair_config.pair);
                            let _: () = con.lpush(&key, json.clone())?;
                            let _: () = con.ltrim(&key, 0, 23)?; // Keep last 24 entries

                            // Format message
                            let message = format!(
                                "🔔 Price Update\n\nPair: {}\nPrice: ${}\nTime: {}",
                                pair_config.pair,
                                price,
                                timestamp
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
