use crate::SkAndAttachments;
use crate::errors::ForumError;
use crate::types::{Attachment, Comment, CommentItem, ItemKey, Post, PostItem};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, ReturnValue, WriteRequest};
use base64::{Engine as _, engine::general_purpose};
use chrono::Utc;
use serde_dynamo::{from_item, from_items, to_attribute_value, to_item};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone)]
pub struct ForumRepository {
    client: Client,
    forums_table: String,
    members_table: String,
}

impl ForumRepository {
    pub fn new(client: Client, forums_table: String, members_table: String) -> Self {
        Self {
            client,
            forums_table,
            members_table,
        }
    }

    pub async fn create_post(
        &self,
        group_id: &str,
        author_id: &str,
        title: &str,
        body: &str,
        attachments: Vec<Attachment>,
    ) -> Result<Post, ForumError> {
        let post_id = Uuid::new_v4().to_string();
        let created_at = Utc::now().to_rfc3339();

        let item = PostItem {
            pk: format!("POST#{post_id}"),
            sk: "METADATA".to_string(),
            gsi1pk: format!("GROUP#{group_id}"),
            gsi1sk: format!("POST#{created_at}#{post_id}"),
            post_id: post_id.clone(),
            group_id: group_id.to_string(),
            author_id: author_id.to_string(),
            title: title.to_string(),
            body: body.to_string(),
            attachments,
            created_at,
            deleted: false,
            deleted_at: None,
        };

        let dynamo_item = to_item(&item)?;

        self.client
            .put_item()
            .table_name(&self.forums_table)
            .set_item(Some(dynamo_item))
            .condition_expression("attribute_not_exists(PK)")
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(item.into())
    }

    pub async fn get_post(&self, post_id: &str) -> Result<Post, ForumError> {
        let key = ItemKey {
            pk: format!("POST#{post_id}"),
            sk: "METADATA".to_string(),
        };
        let key = to_item(&key)?;

        let resp = self
            .client
            .get_item()
            .table_name(&self.forums_table)
            .set_key(Some(key))
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        let item = resp
            .item
            .ok_or_else(|| ForumError::PostNotFound(post_id.to_string()))?;

        let post_item: PostItem = from_item(item)?;

        if post_item.deleted {
            return Err(ForumError::PostNotFound(post_id.to_string()));
        }

        Ok(post_item.into())
    }

    pub async fn update_post(
        &self,
        post_id: &str,
        user_id: &str,
        title: &str,
        body: &str,
        attachments: Vec<Attachment>,
    ) -> Result<(Post, Vec<Attachment>), ForumError> {
        let updated_at = Utc::now().to_rfc3339();
        let attachments_av = to_attribute_value(&attachments)?;

        let result = self
            .client
            .update_item()
            .table_name(&self.forums_table)
            .key("PK", AttributeValue::S(format!("POST#{post_id}")))
            .key("SK", AttributeValue::S("METADATA".to_string()))
            .update_expression("SET #title = :title, #body = :body, updated_at = :updated_at, attachments = :attachments")
            .condition_expression("author_id = :author_id")
            .expression_attribute_names("#title", "title")
            .expression_attribute_names("#body", "body")
            .expression_attribute_values(":title", AttributeValue::S(title.to_string()))
            .expression_attribute_values(":body", AttributeValue::S(body.to_string()))
            .expression_attribute_values(":updated_at", AttributeValue::S(updated_at))
            .expression_attribute_values(":attachments", attachments_av)
            .expression_attribute_values(":author_id", AttributeValue::S(user_id.to_string()))
            .return_values(ReturnValue::AllOld)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        let old_item = result
            .attributes
            .ok_or_else(|| ForumError::PostNotFound(post_id.to_string()))?;

        let post_item: PostItem = from_item(old_item)?;
        let old_attachments = post_item.attachments.clone();

        let new_post = Post {
            title: title.to_string(),
            body: body.to_string(),
            attachments,
            ..post_item.into()
        };

        Ok((new_post, old_attachments))
    }

    pub async fn delete_post(
        &self,
        post_id: &str,
        user_id: &str,
    ) -> Result<Vec<Attachment>, ForumError> {
        let result = self
            .client
            .update_item()
            .table_name(&self.forums_table)
            .key("SK", AttributeValue::S("METADATA".to_string()))
            .key("PK", AttributeValue::S(format!("POST#{post_id}")))
            .update_expression("SET deleted = :true, deleted_at = :deleted_at")
            .condition_expression("author_id = :author_id AND deleted = :false")
            .expression_attribute_values(":true", AttributeValue::Bool(true))
            .expression_attribute_values(":false", AttributeValue::Bool(false))
            .expression_attribute_values(":deleted_at", AttributeValue::S(Utc::now().to_rfc3339()))
            .expression_attribute_values(":author_id", AttributeValue::S(user_id.to_string()))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        let updated_item = result
            .attributes
            .ok_or_else(|| ForumError::PostNotFound(post_id.to_string()))?;
        let post_item: PostItem = from_item(updated_item)?;

        Ok(post_item.attachments)
    }

    pub async fn create_comment(
        &self,
        post_id: &str,
        author_id: &str,
        body: &str,
        attachments: Vec<Attachment>,
    ) -> Result<Comment, ForumError> {
        let comment_id = Uuid::new_v4().to_string();
        let created_at = Utc::now().to_rfc3339();

        let item = CommentItem {
            pk: format!("POST#{post_id}"),
            sk: format!("COMMENT#{created_at}#{comment_id}"),
            comment_id: comment_id.clone(),
            post_id: post_id.to_string(),
            author_id: author_id.to_string(),
            body: body.to_string(),
            attachments,
            created_at,
        };

        let dynamo_item = to_item(&item)?;

        self.client
            .put_item()
            .table_name(&self.forums_table)
            .set_item(Some(dynamo_item))
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(item.into())
    }

    pub async fn update_comment(
        &self,
        post_id: &str,
        comment_sk: &str,
        user_id: &str,
        body: &str,
        attachment: Vec<Attachment>,
    ) -> Result<(Comment, Vec<Attachment>), ForumError> {
        let attachments_av = to_attribute_value(&attachment)?;

        let result = self
            .client
            .update_item()
            .table_name(&self.forums_table)
            .key("PK", AttributeValue::S(format!("POST#{post_id}")))
            .key("SK", AttributeValue::S(comment_sk.to_string()))
            .update_expression("SET #body = :body, attachments = :attachments")
            .condition_expression("author_id = :author_id")
            .expression_attribute_names("#body", "body")
            .expression_attribute_values(":body", AttributeValue::S(body.to_string()))
            .expression_attribute_values(":author_id", AttributeValue::S(user_id.to_string()))
            .expression_attribute_values(":attachments", attachments_av)
            .return_values(ReturnValue::AllOld)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        let old_items = result
            .attributes
            .ok_or_else(|| ForumError::CommentNotFound(comment_sk.to_string()))?;
        let old_item: CommentItem = from_item(old_items)?;
        let old_attachments = old_item.attachments.to_owned();

        let new_comment = Comment {
            body: body.to_string(),
            attachments: attachment,
            ..old_item.into()
        };

        Ok((new_comment, old_attachments))
    }

    pub async fn delete_comment(
        &self,
        post_id: &str,
        comment_sk: &str,
        user_id: &str,
    ) -> Result<Vec<Attachment>, ForumError> {
        let result = self
            .client
            .delete_item()
            .table_name(&self.forums_table)
            .key("PK", AttributeValue::S(format!("POST#{post_id}")))
            .key("SK", AttributeValue::S(comment_sk.to_string()))
            .condition_expression("author_id = :author_id")
            .expression_attribute_values(":author_id", AttributeValue::S(user_id.to_string()))
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        let deleted_item = result
            .attributes
            .ok_or_else(|| ForumError::CommentNotFound(comment_sk.to_string()))?;
        let comment_item: CommentItem = from_item(deleted_item)?;

        Ok(comment_item.attachments)
    }

    pub async fn get_post_page(
        &self,
        group_id: &str,
        cursor: Option<String>,
        limit: Option<i32>,
    ) -> Result<(Vec<Post>, Option<String>), ForumError> {
        let limit = limit.unwrap_or(20);
        let gsi1pk = to_attribute_value(format!("GROUP#{group_id}"))?;

        let mut query = self
            .client
            .query()
            .table_name(&self.forums_table)
            .index_name("gsi1")
            .key_condition_expression("GSI1PK = :gsi1pk")
            .expression_attribute_values(":gsi1pk", gsi1pk)
            .filter_expression("deleted = :false_val")
            .expression_attribute_values(":false_val", AttributeValue::Bool(false))
            .scan_index_forward(false)
            .limit(limit);

        if let Some(cursor) = cursor {
            query = query.set_exclusive_start_key(Some(decode_cursor(&cursor)?));
        }

        let resp = query.send().await.map_err(|e| e.into_service_error())?;

        let Some(items) = resp.items else {
            return Ok((Vec::new(), None));
        };

        let posts = from_items(items)?;

        let next_cursor = resp.last_evaluated_key.map(encode_cursor).transpose()?;

        Ok((posts, next_cursor))
    }

    pub async fn get_comment_page(
        &self,
        post_id: &str,
        cursor: Option<String>,
        limit: Option<i32>,
    ) -> Result<(Vec<Comment>, Option<String>), ForumError> {
        let limit = limit.unwrap_or(20);
        let pk = to_attribute_value(format!("POST#{post_id}"))?;
        let prefix = to_attribute_value("COMMENT#".to_string())?;

        let mut query = self
            .client
            .query()
            .table_name(&self.forums_table)
            .key_condition_expression("PK = :pk AND begins_with(SK, :prefix)")
            .expression_attribute_values(":pk", pk)
            .expression_attribute_values(":prefix", prefix)
            .scan_index_forward(true)
            .limit(limit);

        if let Some(cursor) = cursor {
            query = query.set_exclusive_start_key(Some(decode_cursor(&cursor)?));
        }

        let resp = query.send().await.map_err(|e| e.into_service_error())?;

        let Some(items) = resp.items else {
            return Ok((Vec::new(), None));
        };

        let comments = from_items(items)?;

        let next_cursor = resp.last_evaluated_key.map(encode_cursor).transpose()?;

        Ok((comments, next_cursor))
    }

    pub async fn get_all_comments_for_deletion(
        &self,
        post_id: &str,
    ) -> Result<Vec<(String, Vec<Attachment>)>, ForumError> {
        let pk: AttributeValue = to_attribute_value(format!("POST#{post_id}"))?;
        let prefix: AttributeValue = to_attribute_value("COMMENT#".to_string())?;

        let mut out = Vec::new();
        let mut exclusive_start_key = None;

        loop {
            let mut query = self
                .client
                .query()
                .table_name(&self.forums_table)
                .key_condition_expression("PK = :pk AND begins_with(SK, :prefix)")
                .expression_attribute_values(":pk", pk.clone())
                .expression_attribute_values(":prefix", prefix.clone())
                .projection_expression("SK, attachments");

            if let Some(key) = exclusive_start_key.take() {
                query = query.set_exclusive_start_key(Some(key));
            }

            let resp = query.send().await.map_err(|e| e.into_service_error())?;

            if let Some(items) = resp.items {
                let page: Vec<SkAndAttachments> = from_items(items)?;
                out.extend(page.into_iter().map(|sk_and_attachments| {
                    (
                        sk_and_attachments.sk,
                        sk_and_attachments.attachments.unwrap_or_else(Vec::new),
                    )
                }));
            }

            match resp.last_evaluated_key {
                Some(key) => exclusive_start_key = Some(key),
                None => break,
            }
        }

        Ok(out)
    }

    pub async fn finalize_post_deletion(
        &self,
        post_id: &str,
        comment_sks: &[String],
    ) -> Result<(), ForumError> {
        let mut all_sks: Vec<String> = comment_sks.to_vec();
        all_sks.push("METADATA".to_string());

        for chunk in all_sks.chunks(10) {
            let mut requests: Vec<WriteRequest> = chunk
                .iter()
                .map(|sk| {
                    let key = HashMap::from([
                        (
                            "PK".to_string(),
                            AttributeValue::S(format!("POST#{post_id}")),
                        ),
                        ("SK".to_string(), AttributeValue::S(sk.to_string())),
                    ]);
                    WriteRequest::builder()
                        .delete_request(
                            DeleteRequest::builder()
                                .set_key(Some(key))
                                .build()
                                .expect("key always set"),
                        )
                        .build()
                })
                .collect();

            let mut attempt = 0;
            loop {
                let resp = self
                    .client
                    .batch_write_item()
                    .request_items(&self.forums_table, requests.clone())
                    .send()
                    .await
                    .map_err(|e| e.into_service_error())?;

                match resp.unprocessed_items {
                    Some(unprocessed) if !unprocessed.is_empty() => {
                        requests = unprocessed.into_values().flatten().collect();
                        attempt += 1;
                        if attempt > 5 {
                            return Err(ForumError::General(
                                "Failed to finalize post deletion after multiple attempts".into(),
                            ));
                        }
                        let backoff_ms = 50u64 * 2u64.pow(attempt);

                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    }
                    _ => break,
                }
            }
        }

        Ok(())
    }

    pub async fn assert_group_member(
        &self,
        group_id: &str,
        user_id: &str,
    ) -> Result<(), ForumError> {
        let resp = self
            .client
            .get_item()
            .table_name(&self.members_table)
            .key("PK", AttributeValue::S(format!("GROUP#{group_id}")))
            .key("SK", AttributeValue::S(format!("MEMBER#{user_id}")))
            .projection_expression("#s")
            .expression_attribute_names("#s", "status")
            .consistent_read(false)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        match resp.item {
            Some(item) => {
                let status = item
                    .get("status")
                    .and_then(|v| v.as_s().ok())
                    .map(|s| s.as_str());

                if status == Some("active") {
                    Ok(())
                } else {
                    Err(ForumError::General(format!(
                        "user {} is not an active member of group {}",
                        user_id, group_id
                    )))
                }
            }
            None => Err(ForumError::General(format!(
                "user {} is not a member of group {}",
                user_id, group_id
            ))),
        }
    }
}

fn encode_cursor(key: HashMap<String, AttributeValue>) -> Result<String, ForumError> {
    let value: serde_json::Value = from_item(key)?;
    let bytes = serde_json::to_vec(&value)?;
    Ok(general_purpose::STANDARD.encode(bytes))
}

fn decode_cursor(cursor: &str) -> Result<HashMap<String, AttributeValue>, ForumError> {
    let bytes = general_purpose::STANDARD.decode(cursor)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    Ok(to_item(&value)?)
}
