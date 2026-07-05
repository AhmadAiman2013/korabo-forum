use crate::errors::ForumError;
use crate::types::{Attachment, PresignedUpload};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::presigning::PresigningConfig;
use base64::{Engine as _, engine::general_purpose};
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const MAX_UPLOAD_BYTES: i64 = 4 * 1024 * 1024; // 4MB cap

#[derive(Clone)]
pub struct S3Uploader {
    pub store: S3Store,
    pub cdn: CdnSigner,
}

impl S3Uploader {
    pub fn new(store: S3Store, cdn: CdnSigner) -> Self {
        Self { store, cdn }
    }
}

#[derive(Clone)]
pub struct S3Store {
    client: S3Client,
    bucket: String,
}

impl S3Store {
    pub fn new(client: S3Client, bucket: String) -> Self {
        Self { client, bucket }
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
}

#[derive(Clone)]
pub struct CdnSigner {
    cdn_domain: String,
    cf_private_key: SigningKey,
    cf_key_pair_id: String,
    cf_url_ttl_secs: u64,
}

impl CdnSigner {
    pub fn new(
        cdn_domain: String,
        cf_private_key: SigningKey,
        cf_key_pair_id: String,
        cf_url_ttl_secs: u64,
    ) -> Self {
        Self {
            cdn_domain,
            cf_private_key,
            cf_key_pair_id,
            cf_url_ttl_secs,
        }
    }

    pub fn sign_url(&self, key: &str) -> Result<String, ForumError> {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + self.cf_url_ttl_secs;

        let resource_url = format!("https://{}/{}", self.cdn_domain, key);
        let policy = format!(
            r#"{{"Statement":[{{"Resource":"{resource_url}","Condition":{{"DateLessThan":{{"AWS:EpochTime":{expires}}}}}}}]}}"#
        );

        let signature: Signature = self.cf_private_key.sign(policy.as_bytes());

        let cf_b64 = |bytes: &[u8]| {
            general_purpose::STANDARD
                .encode(bytes)
                .replace('+', "-")
                .replace('=', "_")
                .replace('/', "~")
        };

        let policy_b64 = cf_b64(policy.as_bytes());
        // CloudFront expects the DER-encoded signature for ECDSA keys
        let sig_b64 = cf_b64(signature.to_der().as_bytes());

        Ok(format!(
            "{resource_url}?Policy={policy_b64}&Signature={sig_b64}&Key-Pair-Id={}",
            self.cf_key_pair_id
        ))
    }

    pub fn hydrate(&self, mut attachments: Vec<Attachment>) -> Vec<Attachment> {
        for a in attachments.iter_mut() {
            a.url = self.sign_url(&a.key).ok();
        }
        attachments
    }
}
