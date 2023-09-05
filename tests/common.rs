use std::env;

use minio_rsc::errors::Result;
use minio_rsc::{provider::StaticProvider, Minio};

pub fn get_test_minio() -> Minio {
    dotenv::dotenv().ok();
    let provider = StaticProvider::from_env().unwrap();

    let host = env::var("MINIO_HOST").unwrap_or("localhost:9022".to_owned());

    let virtual_hosted_style = env::var("virtual_hosted_style")
        .map(|f| f.parse().unwrap_or(false))
        .unwrap_or(false);

    let multi_chunked = env::var("multi_chunked")
        .map(|f| f.parse().unwrap_or(false))
        .unwrap_or(false);

    Minio::builder()
        .endpoint(host)
        .provider(provider)
        .virtual_hosted_style(virtual_hosted_style)
        .multi_chunked_encoding(multi_chunked)
        .secure(false)
        .build()
        .unwrap()
}

pub async fn create_bucket_if_not_exist(minio: &Minio, bucket_name: &str) -> Result<()> {
    let exists = minio.bucket_exists(bucket_name).await?;
    if !exists {
        minio.make_bucket(bucket_name, false).await?;
    }
    return Ok(());
}
