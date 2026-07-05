use crate::errors::ForumError;
use crate::types::{Attachment, PresignedUpload};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client as S3Client;
use std::time::Duration;

pub const MAX_UPLOAD_BYTES: i64 = 4 * 1024 * 1024; // 4MB cap

#[derive(Clone)]
pub struct S3Uploader {
    client: S3Client,
    bucket: String,
    cdn_domain: String,
}

impl S3Uploader {
    pub fn new(client: S3Client, bucket: String, cdn_domain: String) -> Self {
        Self {
            client,
            bucket,
            cdn_domain,
        }
    }

    /// Issues a presigned PUT. `content_length` is signed in to the request, so the
    /// client can't upload a different (larger) size than it declared without the
    /// signature becoming invalid — this is what actually enforces the cap, not just
    /// the check below.
    pub async fn presign_upload(
        &self,
        key: &str,
        content_type: &str,
        content_length: i64,
    ) -> Result<PresignedUpload, ForumError> {
        if content_length <= 0 || content_length > MAX_UPLOAD_BYTES {
            return Err(ForumError::General(format!(
                "file size {content_length} bytes exceeds {MAX_UPLOAD_BYTES} byte limit"
            )));
        }

        let expires_in_secs = 300u64;
        let presign_config = PresigningConfig::expires_in(Duration::from_secs(expires_in_secs))?;

        let req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .content_length(content_length)
            .presigned(presign_config)
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(PresignedUpload {
            upload_url: req.uri().to_string(),
            key: key.to_string(),
            expires_in_secs,
        })
    }

    pub async fn delete(&self, keys: &Vec<String>) -> Result<(), ForumError> {
        for key in keys {
            self.client
            .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| e.into_service_error())?;
        }
        Ok(())
    }

    pub fn attachment_url(&self, key: &str) -> String {
        format!("https://{}/{}", self.cdn_domain, key)
    }

    pub fn hydrate(&self, mut attachments: Vec<Attachment>) -> Vec<Attachment> {
        for a in attachments.iter_mut() {
            a.url = Some(self.attachment_url(&a.key));
        }
        attachments
    }
}
