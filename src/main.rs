#[macro_use]
extern crate log;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures::future::try_join_all;
use itertools::Itertools;
use reqwest::StatusCode;
use std::path::PathBuf;
use std::time::Duration;
use tetr_ch::model::record::Record;
use tetr_ch::model::stream::StreamResponse;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    #[arg(short = 'C', long)]
    directory: Option<PathBuf>,
    #[arg(short, long)]
    remove: bool,
    #[arg(required = true)]
    streams: Vec<String>,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

async fn fetch_stream(client: &reqwest::Client, stream: &str) -> Result<Vec<Record>> {
    let response: StreamResponse = client
        .get(format!("https://ch.tetr.io/api/streams/{}", stream))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let records = response.data.context("Missing stream data")?.records;
    info!("Fetched {} records from {}", records.len(), stream);
    Ok(records)
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct Replay {
    id: String,
    is_multi: bool,
    timestamp: DateTime<Utc>,
}

impl Replay {
    fn filename(&self) -> PathBuf {
        let extension = if self.is_multi { "ttrm" } else { "ttr" };
        let timestamp = self.timestamp.format("%Y%m%dT%H%M%SZ");
        format!("{}-{}.{}", timestamp, self.id, extension).into()
    }

    fn url(&self) -> String {
        format!("https://inoue.szy.lol/api/replay/{}", self.id)
    }
}

impl TryFrom<Record> for Replay {
    type Error = chrono::ParseError;

    fn try_from(value: Record) -> std::result::Result<Self, chrono::ParseError> {
        Ok(Replay {
            id: value.replay_id,
            is_multi: value.is_multi.unwrap_or_default(),
            timestamp: DateTime::parse_from_rfc3339(&value.recorded_at)?.with_timezone(&Utc),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    env_logger::init_from_env(
        env_logger::Env::default().default_filter_or(match args.verbose {
            0 => "info",
            1 => "debug",
            _ => "trace",
        }),
    );
    debug!("{:#?}", args);

    let client = reqwest::Client::builder().user_agent(USER_AGENT).build()?;

    let replays: Vec<Replay> = try_join_all(
        args.streams
            .iter()
            .map(|stream| fetch_stream(&client, &stream)),
    )
    .await?
    .concat()
    .into_iter()
    .map(Replay::try_from)
    .try_collect()?;
    let replays = replays.into_iter().unique().collect_vec();

    let directory = args.directory.unwrap_or(".".into());
    if !fs::try_exists(&directory).await? {
        fs::create_dir(&directory).await?;
    }

    let to_keep = replays
        .iter()
        .map(|x| directory.join(x.filename()))
        .collect_vec();

    let to_download = replays
        .into_iter()
        .zip(try_join_all(to_keep.iter().map(fs::try_exists)).await?)
        .filter_map(|(r, e)| match e {
            true => None,
            false => Some(r),
        })
        .collect_vec();

    info!("Downloading {} missing replays", to_download.len());
    let mut hit_limit = false;
    // not using try_join_all as the backend is synchronous
    for replay in to_download {
        let filename = replay.filename();
        let path = directory.join(&filename);
        let url = replay.url();

        let response = loop {
            if hit_limit {
                sleep(Duration::from_secs(5)).await;
            }
            let response = client.get(&url).send().await?;
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                if hit_limit {
                    warn!("Inoue returned 429");
                } else {
                    hit_limit = true;
                    warn!("Inoue returned 429 - adding a 5 second delay");
                }
            } else {
                break response.error_for_status()?;
            }
        };

        let mut file = fs::File::create(&path).await?;
        file.write_all(&response.bytes().await?).await?;
        info!("Downloaded {}", &filename.display());
    }

    let mut existing = Vec::new();
    let mut entries = fs::read_dir(&directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if let Some(extension) = path.extension() {
            if extension == "ttr" || extension == "ttrm" {
                existing.push(path);
            }
        }
    }

    if args.remove {
        let to_remove = existing
            .iter()
            .filter(|x| !to_keep.contains(x))
            .collect_vec();
        info!("Removing {} replays", to_remove.len());
        try_join_all(to_remove.iter().map(fs::remove_file)).await?;
    }

    Ok(())
}
