use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use forum_core::{Attachment, CreateCommentRequest, CreatePostRequest, ForumError, ForumEvent, ForumRepository, ListCommentsRequest, ListPostsRequest, PresignUploadRequest, ResponseError, S3Uploader, SqsClient, UpdateCommentRequest, UpdatePostRequest};
use jwt::{AuthClaims, JwtPublicKey};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::mem::take;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<ForumRepository>,
    pub s3: S3Uploader,
    pub queue: SqsClient,
    pub jwt: JwtPublicKey,
}

impl AsRef<JwtPublicKey> for AppState {
    fn as_ref(&self) -> &JwtPublicKey {
        &self.jwt
    }
}

pub async fn health_check() -> Json<Value> {
    let health = true;
    match health {
        true => Json(json!({ "status": "healthy" })),
        false => Json(json!({ "status": "unhealthy" })),
    }
}

pub async fn create_post_handler(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Json(body): Json<CreatePostRequest>,
) -> Result<(StatusCode, Json<Value>), ResponseError> {
    let user_id = &claims.sub;
    let CreatePostRequest {
        group_id,
        title,
        body,
        attachments,
    } = body;

    state.repo.assert_group_member(&group_id, user_id).await?;

    let mut post = state
        .repo
        .create_post(&group_id, user_id, &title, &body, attachments)
        .await?;

    post.attachments = state.s3.cdn.hydrate(post.attachments);

    state
        .queue
        .publish_sqs_event(&ForumEvent::PostCreated {
            post_id: post.post_id.clone(),
            group_id: post.group_id.clone(),
            author_id: post.author_id.clone(),
            title: post.title.clone(),
        })
        .await?;

    Ok((StatusCode::CREATED, Json(json!({ "body": post }))))
}

pub async fn update_post_handler(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path(post_id): Path<String>,
    Json(body): Json<UpdatePostRequest>,
) -> Result<(StatusCode, Json<Value>), ResponseError> {
    let user_id = &claims.sub;
    let UpdatePostRequest {
        title,
        body,
        attachments,
    } = body;

    let post = state.repo.get_post(&post_id).await?;
    state.repo.assert_group_member(&post.group_id, user_id).await?;

    let (mut post, old_attachments) = state
        .repo
        .update_post(&post_id, user_id, &title, &body, attachments)
        .await?;

    post.attachments = state.s3.cdn.hydrate(post.attachments);

    state
        .queue
        .publish_sqs_event(&ForumEvent::PostUpdated {
            post_id: post.post_id.clone(),
            group_id: post.group_id.clone(),
            author_id: post.author_id.clone(),
            title: post.title.clone(),
        })
        .await?;

    let new_keys: HashSet<String> = post.attachments.iter().map(|a| a.key.clone()).collect();

    let deleted_attachments: Vec<Attachment> = old_attachments
        .into_iter()
        .filter(|a| !new_keys.contains(&a.key))
        .collect();

    send_deleted_attachment_sqs(&state, deleted_attachments).await?;

    Ok((StatusCode::OK, Json(json!({ "body": post }))))
}

pub async fn list_posts_handler(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Query(params): Query<ListPostsRequest>,
) -> Result<(StatusCode, Json<Value>), ResponseError> {
    let user_id = &claims.sub;
    let ListPostsRequest {
        group_id,
        cursor,
        limit,
    } = params;

    state.repo.assert_group_member(&group_id, user_id).await?;

    let (mut posts, next_cursor) = state.repo.get_post_page(&group_id, cursor, limit).await?;

    posts.iter_mut().for_each(|p| {
        p.attachments = state.s3.cdn.hydrate(take(&mut p.attachments));
    });

    Ok((
        StatusCode::OK,
        Json(json!({
            "body": {
                "group_id": group_id,
                "posts": posts,
                "next_cursor": next_cursor,
            }
        })),
    ))
}

pub async fn delete_post_handler(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path(post_id): Path<String>,
) -> Result<StatusCode, ResponseError> {
    let user_id = &claims.sub;

    let post = state.repo.get_post(&post_id).await?;
    state.repo.assert_group_member(&post.group_id, user_id).await?;

    let deleted_attachments = state.repo.delete_post(&post_id, &user_id).await?;

    state
        .queue
        .publish_sqs_event(&ForumEvent::PostDeleted {
            post_id: post_id.clone(),
            post_attachments: deleted_attachments,
        })
        .await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn create_comment_handler(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path(post_id): Path<String>,
    Json(body): Json<CreateCommentRequest>,
) -> Result<(StatusCode, Json<Value>), ResponseError> {
    let user_id = &claims.sub;
    let CreateCommentRequest {
        body,
        attachments,
    } = body;

    let post = state.repo.get_post(&post_id).await?;
    state.repo.assert_group_member(&post.group_id, user_id).await?;

    let mut comment = state
        .repo
        .create_comment(&post_id, user_id, &body, attachments)
        .await?;

    comment.attachments = state.s3.cdn.hydrate(comment.attachments);

    state
        .queue
        .publish_sqs_event(&ForumEvent::CommentCreated {
            comment_id: comment.comment_id.clone(),
            post_id: post_id.clone(),
            group_id: post.group_id.clone(),
            author_id: comment.author_id.clone(),
        })
        .await?;

    Ok((StatusCode::CREATED, Json(json!({ "body": comment }))))
}

pub async fn list_comments_handler(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path(post_id): Path<String>,
    Query(params): Query<ListCommentsRequest>,
) -> Result<(StatusCode, Json<Value>), ResponseError> {
    let user_id = &claims.sub;
    let ListCommentsRequest {
        group_id,
        cursor,
        limit,
    } = params;

    state.repo.assert_group_member(&group_id, user_id).await?;

    let (mut comments, next_cursor) = state
        .repo
        .get_comment_page(&post_id, cursor, limit)
        .await?;

    comments.iter_mut().for_each(|c| {
        c.attachments = state.s3.cdn.hydrate(take(&mut c.attachments));
    });

    Ok((
        StatusCode::OK,
        Json(json!({
            "body": {
                "post_id": post_id,
                "comments": comments,
                "next_cursor": next_cursor,
            }
        })),
    ))
}

pub async fn update_comment_handler(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path((post_id, comment_sk)): Path<(String, String)>,
    Json(body): Json<UpdateCommentRequest>,
) -> Result<(StatusCode, Json<Value>), ResponseError> {
    let user_id = &claims.sub;
    let UpdateCommentRequest {
        body,
        attachments,
    } = body;

    let post = state.repo.get_post(&post_id).await?;
    state.repo.assert_group_member(&post.group_id, user_id).await?;

    let (mut comment, old_attachments) = state
        .repo
        .update_comment(&post_id, &comment_sk, user_id, &body, attachments)
        .await?;

    comment.attachments = state.s3.cdn.hydrate(comment.attachments);

    state
        .queue
        .publish_sqs_event(&ForumEvent::CommentUpdated {
            comment_id: comment.comment_id.clone(),
            post_id: post_id.to_owned(),
            group_id: post.group_id.clone(),
            author_id: comment.author_id.clone(),
        })
        .await?;

    let new_keys: HashSet<String> = comment.attachments.iter().map(|a| a.key.clone()).collect();

    let deleted_attachments: Vec<Attachment> = old_attachments
        .into_iter()
        .filter(|a| !new_keys.contains(&a.key))
        .collect();

    send_deleted_attachment_sqs(&state, deleted_attachments).await?;

    Ok((StatusCode::OK, Json(json!({ "body": comment }))))
}

pub async fn delete_comment_handler(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path((post_id, comment_sk)): Path<(String, String)>,
) -> Result<StatusCode, ResponseError> {
    let user_id = &claims.sub;

    let post = state.repo.get_post(&post_id).await?;
    state.repo.assert_group_member(&post.group_id, user_id).await?;

    let attachments = state
        .repo
        .delete_comment(&post_id, &comment_sk, &user_id)
        .await?;

    send_deleted_attachment_sqs(&state, attachments).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn presign_upload_handler(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Json(body): Json<PresignUploadRequest>,
) -> Result<(StatusCode, Json<Value>), ResponseError> {
    let user_id = &claims.sub;
    let PresignUploadRequest {
        group_id,
        file_name,
        content_type,
        content_length,
    } = body;

    state.repo.assert_group_member(&group_id, user_id).await?;

    let key = format!(
        "forum-uploads/{group_id}/{user_id}/{}-{}",
        uuid::Uuid::new_v4(),
        file_name
    );

    let presigned = state
        .s3
        .store
        .presign_upload(&key, &content_type, content_length)
        .await?;

    Ok((StatusCode::OK, Json(json!({ "body": presigned }))))
}

async fn send_deleted_attachment_sqs(
    state: &AppState,
    attachments: Vec<Attachment>,
) -> Result<(), ForumError> {
    if !attachments.is_empty() {
        let keys = attachments.iter().map(|a| a.key.clone()).collect();

        state
            .queue
            .publish_sqs_event(&ForumEvent::AttachmentsDeleted {
                deleted_attachments: keys,
            })
            .await?;
    }
    Ok(())
}
