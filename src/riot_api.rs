use std::env;
use std::error::Error;

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;

const ACCOUNT_BASE_URL: &str = "https://europe.api.riotgames.com/riot/account/v1";
const MATCH_BASE_URL: &str = "https://europe.api.riotgames.com/lol/match/v5";

#[derive(Deserialize)]
struct AccountResponse {
    puuid: String,
}

fn build_headers() -> Result<HeaderMap, Box<dyn Error>> {
    let api_key = env::var("RIOT_API_KEY")?;
    let mut headers = HeaderMap::new();
    headers.insert("X-Riot-Token", HeaderValue::from_str(&api_key)?);
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(headers)
}

pub async fn get_puuid(game_name: &str, tag_line: &str) -> Result<String, Box<dyn Error>> {
    let url = format!(
        "{}/accounts/by-riot-id/{}/{}",
        ACCOUNT_BASE_URL, game_name, tag_line
    );

    let client = reqwest::Client::new();
    let headers = build_headers()?;

    let response = client
        .get(url)
        .headers(headers)
        .send()
        .await?
        .error_for_status()?;

    let account: AccountResponse = response.json().await?;
    Ok(account.puuid)
}

pub async fn get_match_ids_by_puuid(
    puuid: &str,
    count: usize,
) -> Result<Vec<String>, Box<dyn Error>> {
    let url = format!(
        "{}/matches/by-puuid/{}/ids?start=0&count={}",
        MATCH_BASE_URL, puuid, count
    );

    let client = reqwest::Client::new();
    let headers = build_headers()?;

    let response = client
        .get(url)
        .headers(headers)
        .send()
        .await?
        .error_for_status()?;

    let ids: Vec<String> = response.json().await?;
    Ok(ids)
}
