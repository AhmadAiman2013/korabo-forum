use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
use aws_sdk_dynamodb::Error as DynamoError;
use aws_sdk_s3::operation::put_object::PutObjectError as UploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError as DeleteError;
use aws_sdk_s3::presigning::PresigningConfigError;
use aws_sdk_sqs::operation::send_message::SendMessageError;
use aws_sdk_ssm::error::SdkError;
use aws_sdk_ssm::operation::get_parameter::GetParameterError;
use axum::http::StatusCode;
use axum::Json;
use axum::response::{IntoResponse, Response};
use base64::DecodeError;
use lambda_http::tracing::error;
use serde_dynamo::Error;
use serde_json::{json, Error as SerdeJsonError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ForumError {
    #[error("DynamoDB error: {0}")]
    DynamoDB(#[from] DynamoError),

    #[error("DynamoDB put item error: {0}")]
    PutItem(#[from] PutItemError),

    #[error("DynamoDB get item error: {0}")]
    GetItem(#[from] GetItemError),

    #[error("Query error: {0}")]
    QueryError(#[from] QueryError),

    #[error("Update error: {0}")]
    UpdateError(#[from] UpdateItemError),

    #[error("Delete error: {0}")]
    DeleteError(#[from] DeleteItemError),
    
    #[error("Batch write error: {0}")]
    BatchWriteError(#[from] BatchWriteItemError),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] Error),

    #[error("Deserialization error: {0}")]
    SerdeSerialization(#[from] SerdeJsonError),

    #[error("Post not found: {0}")]
    PostNotFound(String),

    #[error("Comment not found: {0}")]
    CommentNotFound(String),

    #[error("Decode error: {0}")]
    Internal(#[from] DecodeError),

    #[error("General error: {0}")]
    General(String),

    #[error("Upload error: {0}")]
    UploadError(#[from] UploadError),

    #[error("Delete Object error: {0}")]
    DeleteObjectError(#[from] DeleteError),

    #[error("Presign error: {0}")]
    PresignError(#[from] PresigningConfigError),

    #[error("SQS error: {0}")]
    SQSError(#[from] SendMessageError),

    #[error("Not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Error)]
pub enum ResponseError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Forbidden")]
    Forbidden,
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("Internal server error")]
    Internal(String),

    #[error("{0}")]
    ForumError(#[from] ForumError),
    
    #[error("SSM error: {0}")]
    GetParameterError(#[from] SdkError<GetParameterError>),
}

impl IntoResponse for ResponseError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            ResponseError::NotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            ResponseError::Forbidden => (StatusCode::FORBIDDEN, self.to_string()),
            ResponseError::Conflict(_) => (StatusCode::CONFLICT, self.to_string()),
            ResponseError::BadRequest(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            ResponseError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            ResponseError::ForumError(err) => {
                error!("Forum error: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error".to_string(),
                )
            },
            ResponseError::GetParameterError(_) => {
                error!("SSM error: {:?}", self);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error".to_string(),
                )
            }
        };
        (status, Json(json!({ "error": body }))).into_response()
    }
}

