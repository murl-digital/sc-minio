use std::pin::Pin;

use crate::{
    errors::Result,
    types::{
        args::{BucketArgs, CopySource, ListObjectsArgs, ObjectArgs},
        Object,
    },
    Minio,
};
use async_stream::stream as Stream2;
use futures_core::Stream;
use futures_util::{stream, StreamExt};

/// Added extension operate.
/// All operations are experimental.
impl Minio {
    /// Modify the content_type of an object
    pub async fn set_content_type(&self, mut args: ObjectArgs, content_type: String) -> Result<()> {
        let stat = if let Some(stat) = self.stat_object(args.clone()).await? {
            stat
        } else {
            return Ok(());
        };
        let cs = CopySource::from(args.clone()).metadata_replace(true);
        args = args
            .metadata(stat.metadata)
            .content_type(Some(content_type));
        self.copy_object(args, cs).await
    }

    pub fn list_objects_stream<'a>(
        &'a self,
        args: BucketArgs,
    ) -> Pin<Box<dyn Stream<Item = Result<Object>> + Send + 'a>> {
        let bucket = args.bucket_name;
        let mut args: Option<ListObjectsArgs> = Some(
            ListObjectsArgs::new(bucket.as_str())
                .max_keys(100)
                .prefix("")
                .delimiter(""),
        );
        let stm = Stream2!({
            while let Some(arg) = args.take() {
                let res = self.list_objects(arg).await;
                if let Ok(res) = &res {
                    if res.is_truncated {
                        args = Some(
                            ListObjectsArgs::new(bucket.as_str())
                                .max_keys(100)
                                .prefix("")
                                .delimiter("")
                                .continuation_token(res.next_continuation_token.as_str()),
                        );
                    }
                }
                yield res
            }
        });
        Box::pin(stm.flat_map(|f| {
            stream::iter(match f {
                Ok(f) => f.contents.into_iter().map(Result::Ok).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            })
        }))
    }
}
