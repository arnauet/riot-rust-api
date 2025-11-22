use reqwest::StatusCode;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::VecDeque;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::{Instant as TokioInstant, sleep, sleep_until};

const BASE_URL: &str = "https://europe.api.riotgames.com";
const DEFAULT_MAX_REQS_PER_2MIN: usize = 80;
const DEFAULT_MAX_REQS_PER_SEC: usize = 20;
static GLOBAL_RATE_LIMITER: OnceLock<Mutex<RateLimiter>> = OnceLock::new();

#[derive(Deserialize)]
pub struct AccountResponse {
    puuid: String,
}

fn build_headers() -> Result<HeaderMap, Box<dyn Error>> {
    let api_key = env::var("RIOT_API_KEY")?;

    let mut headers = HeaderMap::new();
    headers.insert("X-Riot-Token", HeaderValue::from_str(&api_key)?);
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    Ok(headers)
}

pub struct RiotClient {
    client: reqwest::Client,
    headers: HeaderMap,
}

impl RiotClient {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        global_rate_limiter();

        Ok(Self {
            client: reqwest::Client::new(),
            headers: build_headers()?,
        })
    }

    pub fn new_with_max(max_reqs_per_2min: usize) -> Result<Self, Box<dyn Error>> {
        global_rate_limiter();

        {
            let limiter = global_rate_limiter();
            let mut guard = limiter.blocking_lock();
            guard.set_max_reqs_per_2min(max_reqs_per_2min);
        }

        Ok(Self {
            client: reqwest::Client::new(),
            headers: build_headers()?,
        })
    }

    pub async fn get_match_ids_by_puuid(
        &self,
        puuid: &str,
        count: usize,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/lol/match/v5/matches/by-puuid/{}/ids?start=0&count={}",
            BASE_URL, puuid, count
        );

        self.get_json(&url).await
    }

    pub async fn get_match_json(
        &self,
        match_id: &str,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let url = format!("{}/lol/match/v5/matches/{}", BASE_URL, match_id);

        self.get_json(&url).await
    }

    pub async fn get_account_by_riot_id(
        &self,
        game_name: &str,
        tag_line: &str,
    ) -> Result<AccountResponse, Box<dyn Error>> {
        let url = format!(
            "{}/riot/account/v1/accounts/by-riot-id/{}/{}",
            BASE_URL, game_name, tag_line
        );

        self.get_json(&url).await
    }

    async fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T, Box<dyn Error>> {
        let response = self.request_with_retry(url).await?;
        Ok(response.json().await?)
    }

    async fn request_with_retry(&self, url: &str) -> Result<reqwest::Response, Box<dyn Error>> {
        const MAX_ATTEMPTS: usize = 2;
        let mut attempt = 0;

        loop {
            attempt += 1;

            wait_global_rate_limit().await;

            let response = self
                .client
                .get(url)
                .headers(self.headers.clone())
                .send()
                .await?;

            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                if attempt >= MAX_ATTEMPTS {
                    return Err("Received HTTP 429 Too Many Requests twice".into());
                }

                let retry_after = parse_retry_after(&response).unwrap_or(Duration::from_secs(10));
                sleep(retry_after).await;
                continue;
            }

            if !response.status().is_success() {
                return Err(format!("Request failed with status {}", response.status()).into());
            }

            return Ok(response);
        }
    }
}

pub struct RateLimiter {
    max_reqs_per_2min: usize,
    max_reqs_per_sec: usize,
    timestamps_2min: VecDeque<Instant>,
    timestamps_1s: VecDeque<Instant>,
}

impl RateLimiter {
    pub fn new(max_reqs_per_2min: usize, max_reqs_per_sec: usize) -> Self {
        Self {
            max_reqs_per_2min,
            max_reqs_per_sec,
            timestamps_2min: VecDeque::new(),
            timestamps_1s: VecDeque::new(),
        }
    }

    pub fn set_max_reqs_per_2min(&mut self, max_reqs_per_2min: usize) {
        self.max_reqs_per_2min = max_reqs_per_2min;
    }

    pub async fn wait(&mut self) {
        loop {
            let now = Instant::now();
            self.prune(now);

            let mut sleep_until_instant: Option<TokioInstant> = None;

            if self.timestamps_1s.len() >= self.max_reqs_per_sec {
                if let Some(oldest) = self.timestamps_1s.front() {
                    let elapsed = now.duration_since(*oldest);
                    if elapsed < Duration::from_secs(1) {
                        sleep_until_instant =
                            Some(TokioInstant::now() + (Duration::from_secs(1) - elapsed));
                    }
                }
            }

            if sleep_until_instant.is_none() && self.timestamps_2min.len() >= self.max_reqs_per_2min
            {
                if let Some(oldest) = self.timestamps_2min.front() {
                    let elapsed = now.duration_since(*oldest);
                    if elapsed < Duration::from_secs(120) {
                        sleep_until_instant =
                            Some(TokioInstant::now() + (Duration::from_secs(120) - elapsed));
                    }
                }
            }

            if let Some(deadline) = sleep_until_instant {
                sleep_until(deadline).await;
                continue;
            }

            let timestamp = Instant::now();
            self.timestamps_1s.push_back(timestamp);
            self.timestamps_2min.push_back(timestamp);
            break;
        }
    }

    fn prune(&mut self, now: Instant) {
        while let Some(front) = self.timestamps_1s.front() {
            if now.duration_since(*front) > Duration::from_secs(1) {
                self.timestamps_1s.pop_front();
            } else {
                break;
            }
        }

        while let Some(front) = self.timestamps_2min.front() {
            if now.duration_since(*front) > Duration::from_secs(120) {
                self.timestamps_2min.pop_front();
            } else {
                break;
            }
        }
    }
}

fn global_rate_limiter() -> &'static Mutex<RateLimiter> {
    GLOBAL_RATE_LIMITER.get_or_init(|| {
        Mutex::new(RateLimiter::new(
            DEFAULT_MAX_REQS_PER_2MIN,
            DEFAULT_MAX_REQS_PER_SEC,
        ))
    })
}

async fn wait_global_rate_limit() {
    let limiter = global_rate_limiter();
    let mut guard = limiter.lock().await;
    guard.wait().await;
}

fn parse_retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
}

pub async fn get_puuid(game_name: &str, tag_line: &str) -> Result<String, Box<dyn Error>> {
    let client = RiotClient::new()?;
    let account = client.get_account_by_riot_id(game_name, tag_line).await?;
    Ok(account.puuid)
}

pub async fn get_match_ids_by_puuid(
    puuid: &str,
    count: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let client = RiotClient::new()?;
    client.get_match_ids_by_puuid(puuid, count).await
}

pub async fn get_match_json(match_id: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let client = RiotClient::new()?;
    client.get_match_json(match_id).await
}

pub async fn download_and_save_matches(
    puuid: &str,
    count: usize,
    out_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(out_dir)?;

    let match_ids = get_match_ids_by_puuid(puuid, count).await?;
    let total = match_ids.len();

    for (idx, match_id) in match_ids.iter().enumerate() {
        eprintln!("Downloading match {}/{}: {}", idx + 1, total, match_id);

        let match_json = get_match_json(match_id).await?;
        let serialized = serde_json::to_vec_pretty(&match_json)?;
        let file_path = out_dir.join(format!("{}.json", match_id));
        fs::write(file_path, serialized)?;
    }

    Ok(())
}
