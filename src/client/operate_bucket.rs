use hyper::header;
use hyper::Method;

use super::args::ObjectLockConfig;
use super::{BucketArgs, ListObjectVersionsArgs, ListObjectsArgs, Tags};
use crate::datatype::ListAllMyBucketsResult;
use crate::datatype::ListBucketResult;
use crate::datatype::ListVersionsResult;
use crate::datatype::{Bucket, Owner, VersioningConfiguration};
use crate::error::{Error, Result};
use crate::Minio;

macro_rules! get_attr {
    ($name:ident, $query:expr, $T:tt) => {
        #[doc = concat!("Get [",stringify!($T),"] of a bucket")]
        /// ## Example
        /// ```rust
        /// # use minio_rsc::{Minio, error::Result};
        /// # async fn example(minio: Minio) -> Result<()> {
        #[doc = concat!("let config = minio.", stringify!($name), r#"("bucket").await?;"#)]
        /// # Ok(())}
        /// ```
        ///
        #[inline]
        pub async fn $name<B>(&self, bucket: B) -> Result<$T>
        where
            B: Into<BucketArgs>,
        {
            self._bucket_executor(bucket.into(), Method::GET)
                .query($query, "")
                .send_xml_ok()
                .await
        }
    };
}

/// Operating the bucket
impl Minio {
    #[inline]
    pub(crate) fn _bucket_executor(
        &self,
        bucket: BucketArgs,
        method: Method,
    ) -> super::BaseExecutor {
        self.executor(method)
            .bucket_name(&bucket.name)
            .headers_merge2(bucket.extra_headers)
            .apply(|e| {
                if let Some(owner) = &bucket.expected_bucket_owner {
                    e.header("x-amz-expected-bucket-owner", owner)
                } else {
                    e
                }
            })
    }

    /// Check if a bucket exists.
    /// If bucket exists and you have permission to access it, return [Ok(true)], otherwise [Ok(false)]
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::BucketArgs;
    /// # use minio_rsc::Minio;
    /// # use minio_rsc::error::Result;
    /// # async fn example(minio: Minio) -> Result<()>{
    /// let exists:bool = minio.bucket_exists(BucketArgs::new("bucket")).await?;
    /// let exists:bool = minio.bucket_exists("bucket").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn bucket_exists<B>(&self, bucket: B) -> Result<bool>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        self._bucket_executor(bucket, Method::HEAD)
            .send()
            .await
            .map(|res| res.status().is_success())
    }

    /// List information of all accessible buckets.
    /// ## Example
    /// ```rust
    /// # use minio_rsc::Minio;
    /// # async fn example(minio: Minio){
    /// let (buckets, owner) = minio.list_buckets().await.unwrap();
    /// # }
    /// ```
    pub async fn list_buckets(&self) -> Result<(Vec<Bucket>, Owner)> {
        let res = self
            .executor(Method::GET)
            .send_xml_ok::<ListAllMyBucketsResult>()
            .await?;
        Ok((res.buckets.bucket, res.owner))
    }

    /// Lists metadata about all versions of the objects in a bucket.
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::ListObjectVersionsArgs;
    /// # use minio_rsc::Minio;
    /// # async fn example(minio: Minio){
    /// let mut args = ListObjectVersionsArgs::default();
    /// args.max_keys = 100;
    /// minio.list_object_versions("bucket", args).await;
    /// # }
    /// ```
    pub async fn list_object_versions<B>(
        &self,
        bucket: B,
        args: ListObjectVersionsArgs,
    ) -> Result<ListVersionsResult>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        self._bucket_executor(bucket, Method::GET)
            .querys(args.args_query_map())
            .headers_merge2(args.extra_headers)
            .send_xml_ok()
            .await
    }

    /// Lists object information of a bucket.
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::ListObjectsArgs;
    /// # use minio_rsc::Minio;
    /// # async fn example(minio: Minio){
    /// let args = ListObjectsArgs::default().max_keys(10);
    /// minio.list_objects("bucket", args).await;
    /// # }
    /// ```
    pub async fn list_objects<B>(
        &self,
        bucket: B,
        args: ListObjectsArgs,
    ) -> Result<ListBucketResult>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        self._bucket_executor(bucket, Method::GET)
            .querys(args.args_query_map())
            .headers_merge2(args.extra_headers)
            .send_xml_ok()
            .await
    }

    /// Create a bucket with object_lock
    /// ## params
    /// - object_lock: prevents objects from being deleted.
    /// Required to support retention and legal hold.
    /// Can only be enabled at bucket creation.
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::BucketArgs;
    /// # use minio_rsc::Minio;
    /// # async fn example(minio: Minio){
    /// minio.make_bucket(BucketArgs::new("bucket"), true).await;
    /// minio.make_bucket("bucket", false).await;
    /// # }
    /// ```
    pub async fn make_bucket<B>(&self, bucket: B, object_lock: bool) -> Result<String>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        let region = &bucket.region.unwrap_or(self.region().to_string());
        let body = format!("<CreateBucketConfiguration><LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>",region);
        self.executor(Method::PUT)
            .bucket_name(bucket.name)
            .headers_merge2(bucket.extra_headers)
            .apply(|e| {
                if object_lock {
                    e.header("x-amz-bucket-object-lock-enabled", "true")
                } else {
                    e
                }
            })
            .body(body)
            .send_ok()
            .await
            .map(|res| {
                let location = res.headers().get(header::LOCATION);
                if let Some(loc) = location {
                    if let Ok(loc) = loc.to_str() {
                        return Ok(loc.to_string());
                    }
                }
                Err(res.into())
            })?
    }

    /// Remove an **empty** bucket.
    /// If the operation succeeds, return [Ok] otherwise [Error]
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::BucketArgs;
    /// # use minio_rsc::Minio;
    /// # async fn example(minio: Minio){
    /// minio.remove_bucket(BucketArgs::new("bucket")).await;
    /// minio.remove_bucket("bucket").await;
    /// # }
    /// ```
    pub async fn remove_bucket<B>(&self, bucket: B) -> Result<()>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        self._bucket_executor(bucket, Method::DELETE)
            .send_ok()
            .await
            .map(|_| ())
    }

    /// Get [Option]<[Tags]> of a bucket.
    /// Note: return [None] if bucket had not set tagging or delete tagging.
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::BucketArgs;
    /// use minio_rsc::client::Tags;
    /// # use minio_rsc::{Minio, error::Result};
    /// # async fn example(minio: Minio) -> Result<()> {
    /// let tags:Option<Tags> = minio.get_bucket_tags(BucketArgs::new("bucket")).await?;
    /// let tags:Option<Tags> = minio.get_bucket_tags("bucket").await?;
    /// # Ok(())}
    /// ```
    pub async fn get_bucket_tags<B>(&self, bucket: B) -> Result<Option<Tags>>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        let res = self
            ._bucket_executor(bucket, Method::GET)
            .query("tagging", "")
            .send_xml_ok::<Tags>()
            .await;
        match res {
            Ok(tags) => Ok(Some(tags)),
            Err(Error::S3Error(s)) if s.code == "NoSuchTagSet" => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Set tags of a bucket.
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::BucketArgs;
    /// use minio_rsc::client::Tags;
    /// # use minio_rsc::{Minio, error::Result};
    /// # async fn example(minio: Minio) -> Result<()> {
    /// let mut tags = Tags::new();
    /// tags.insert("key1".to_string(), "value1".to_string());
    /// tags.insert("key2".to_string(), "value2".to_string());
    /// tags.insert("key3".to_string(), "value3".to_string());
    /// minio.set_bucket_tags(BucketArgs::new("bucket"), tags).await?;
    ///     let mut tags:Tags = minio.get_bucket_tags(BucketArgs::new("bucket")).await?.unwrap_or(Tags::new());
    /// tags.insert("key4".to_string(), "value4".to_string());
    /// minio.set_bucket_tags("bucket", tags).await?;
    /// # Ok(())}
    /// ```
    pub async fn set_bucket_tags<B>(&self, bucket: B, tags: Tags) -> Result<()>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        self._bucket_executor(bucket, Method::PUT)
            .query("tagging", "")
            .xml(&tags)
            .send_ok()
            .await
            .map(|_| ())
    }

    /// Delete tags of a bucket.
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::BucketArgs;
    /// use minio_rsc::client::Tags;
    /// # use minio_rsc::{Minio, error::Result};
    /// # async fn example(minio: Minio) -> Result<()> {
    /// minio.delete_bucket_tags(BucketArgs::new("bucket")).await?;
    /// minio.delete_bucket_tags("bucket").await?;
    /// # Ok(())}
    /// ```
    pub async fn delete_bucket_tags<B>(&self, bucket: B) -> Result<()>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        self._bucket_executor(bucket, Method::DELETE)
            .query("tagging", "")
            .send_ok()
            .await?;
        Ok(())
    }

    get_attr!(get_bucket_versioning, "versioning", VersioningConfiguration);

    /// Set [VersioningConfiguration] of a bucket.
    /// ## Example
    /// ```rust
    /// use minio_rsc::datatype::{MFADelete, VersioningConfiguration, VersioningStatus};
    /// # use minio_rsc::{Minio, error::Result};
    /// # async fn example(minio: Minio) -> Result<()> {
    /// let versing = VersioningConfiguration{
    ///     mfa_delete:Some(MFADelete::Enabled),
    ///     status:Some(VersioningStatus::Enabled)
    /// };
    /// minio.set_bucket_versioning("bucket", versing).await?;
    /// # Ok(())}
    /// ```
    pub async fn set_bucket_versioning<B>(
        &self,
        bucket: B,
        versioning: VersioningConfiguration,
    ) -> Result<()>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        self._bucket_executor(bucket, Method::PUT)
            .query_string("versioning")
            .xml(&versioning)
            .send_ok()
            .await
            .map(|_| ())
    }

    get_attr!(get_object_lock_config, "object-lock", ObjectLockConfig);

    /// Set [ObjectLockConfig] of a bucket.
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::BucketArgs;
    /// use minio_rsc::client::ObjectLockConfig;
    /// # use minio_rsc::{Minio, error::Result};
    /// # async fn example(minio: Minio) -> Result<()> {
    /// let mut conf = ObjectLockConfig::new(1, true, true);
    /// minio.set_object_lock_config("bucket", conf).await?;
    /// # Ok(())}
    /// ```
    pub async fn set_object_lock_config<B>(&self, bucket: B, config: ObjectLockConfig) -> Result<()>
    where
        B: Into<BucketArgs>,
    {
        let bucket: BucketArgs = bucket.into();
        self._bucket_executor(bucket, Method::PUT)
            .query_string("object-lock")
            .xml(&config)
            .send_ok()
            .await
            .map(|_| ())
    }

    /// Delete [ObjectLockConfig] of a bucket.
    /// ## Example
    /// ```rust
    /// use minio_rsc::client::BucketArgs;
    /// # use minio_rsc::{Minio, error::Result};
    /// # async fn example(minio: Minio) -> Result<()> {
    /// minio.delete_object_lock_config("bucket").await?;
    /// # Ok(())}
    /// ```
    pub async fn delete_object_lock_config<B>(&self, bucket: B) -> Result<()>
    where
        B: Into<BucketArgs>,
    {
        let config = ObjectLockConfig::default();
        self.set_object_lock_config(bucket, config).await
    }
}
