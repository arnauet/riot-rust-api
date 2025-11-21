use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::Value;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

const BASE_URL: &str = "https://europe.api.riotgames.com";

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
        "{}/riot/account/v1/accounts/by-riot-id/{}/{}",
        BASE_URL, game_name, tag_line
    );

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .headers(build_headers()?)
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
    let url = format!(
        "{}/lol/match/v5/matches/by-puuid/{}/ids?start=0&count={}",
        BASE_URL, puuid, count
    );

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .headers(build_headers()?)
        .send()
        .await?
        .error_for_status()?;

    let match_ids: Vec<String> = response.json().await?;
    Ok(match_ids)
}

pub async fn get_match_json(match_id: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let url = format!("{}/lol/match/v5/matches/{}", BASE_URL, match_id);

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .headers(build_headers()?)
        .send()
        .await?
        .error_for_status()?;

    let match_json: Value = response.json().await?;
    Ok(match_json)
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
