use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;
use std::env;
use std::error::Error;

const BASE_URL: &str = "https://europe.api.riotgames.com";

#[derive(Deserialize)]
struct AccountResponse {
    puuid: String,
}

pub async fn get_puuid(game_name: &str, tag_line: &str) -> Result<String, Box<dyn Error>> {
    let api_key = env::var("RIOT_API_KEY")?;

    let url = format!(
        "{}/riot/account/v1/accounts/by-riot-id/{}/{}",
        BASE_URL, game_name, tag_line
    );

    let mut headers = HeaderMap::new();
    headers.insert("X-Riot-Token", HeaderValue::from_str(&api_key)?);
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let client = reqwest::Client::new();
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
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let api_key = env::var("RIOT_API_KEY")?;

    let url = format!(
        "{}/lol/match/v5/matches/by-puuid/{}/ids?start=0&count={}",
        BASE_URL, puuid, count
    );

    let mut headers = HeaderMap::new();
    headers.insert("X-Riot-Token", HeaderValue::from_str(&api_key)?);
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .headers(headers)
        .send()
        .await?
        .error_for_status()?;

    let match_ids: Vec<String> = response.json().await?;
    Ok(match_ids)
}
