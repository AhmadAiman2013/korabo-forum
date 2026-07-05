use crate::forum_handlers::{
    AppState, create_comment_handler, create_post_handler, delete_comment_handler,
    delete_post_handler, health_check, list_comments_handler, list_posts_handler,
    presign_upload_handler, update_comment_handler, update_post_handler,
};
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_s3::Client as S3Client;
use axum::Router;
use axum::http::Method;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::routing::{get, post};
use forum_core::{
    CdnSigner, ForumRepository, S3Store, S3Uploader, SqsClient, get_parameter, get_parameter_secret,
};
use jwt::JwtPublicKey;
use lambda_http::tracing::init_default_subscriber;
use lambda_http::{Error, run};
use p256::SecretKey;
use p256::ecdsa::SigningKey;
use std::env::var;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

mod forum_handlers;

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_default_subscriber();

    let jwt = JwtPublicKey::from_jwks_file(
        var("JWT_ISSUER").expect("JWT_ISSUER must be set"),
        var("JWT_AUDIENCE").expect("JWT_AUDIENCE must be set"),
    )
    .expect("Failed to load JWKS");
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    let client = DynamoClient::new(&config);
    let forums_table = String::from("korabo_forum");
    let members_table = String::from("korabo_group_members");
    let repo = Arc::new(ForumRepository::new(client, forums_table, members_table));

    let ssm_client = aws_sdk_ssm::Client::new(&config);
    let ( ssm_value_1, ssm_value_2, ssm_value_3 ) = tokio::join!(
        get_parameter(&ssm_client, "/korabo/prod/sqs/forum"),
        get_parameter_secret(&ssm_client, "/korabo/prod/sqs/forum-secret"),
        get_parameter(&ssm_client, "/korabo/prod/sqs/forum-queue")
    );

    let ssm_value_1 = ssm_value_1?;
    let ssm_value_2 = ssm_value_2?;
    let ssm_value_3 = ssm_value_3?;
    
    let bucket = ssm_value_1
        .first()
        .cloned()
        .expect("S3 bucket not found in SSM parameter");
    let cf_key_pair_id = ssm_value_1
        .get(1)
        .cloned()
        .expect("CloudFront key pair ID not found in SSM parameter");
    let cdn_domain = ssm_value_1
        .last()
        .cloned()
        .expect("CDN URL not found in SSM parameter");
    let s3_client = S3Client::new(&config);
    let store = S3Store::new(s3_client, bucket);

    let secret_key = SecretKey::from_sec1_pem(&ssm_value_2).expect("Failed to parse secret key");
    let cf_private_key = SigningKey::from(secret_key);
    let cdn = CdnSigner::new(cdn_domain, cf_private_key, cf_key_pair_id, 3600);

    let s3 = S3Uploader::new(store, cdn);

    let sqs = aws_sdk_sqs::Client::new(&config);

    let queue_url_noti = ssm_value_3
        .first()
        .cloned()
        .expect("Noti queue not found in SSM parameter");
    let queue_url_cm_del = ssm_value_3
        .last()
        .cloned()
        .expect("Forum queue URL not found in SSM parameter");
    let queue = SqsClient::new(sqs, queue_url_noti, queue_url_cm_del);

    let origins = [
        "https://d3h6bl8rffsevw.cloudfront.net".parse()?,
        "http://localhost:4200".parse()?,
    ];

    let cors = CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([CONTENT_TYPE, AUTHORIZATION]);

    let state = AppState {
        repo,
        s3,
        queue,
        jwt,
    };

    let app = Router::new()
        .nest(
            "/forum",
            Router::new()
                .route("/health", get(health_check))
                .route("/posts", get(list_posts_handler).post(create_post_handler))
                .route(
                    "/posts/{post_id}",
                    post(update_post_handler).delete(delete_post_handler),
                )
                .route(
                    "/posts/{post_id}/comments",
                    get(list_comments_handler).post(create_comment_handler),
                )
                .route(
                    "/posts/{post_id}/comments/{comment_sk}",
                    post(update_comment_handler).delete(delete_comment_handler),
                )
                .route("/posts/upload", post(presign_upload_handler))
                .with_state(state),
        )
        .layer(cors);

    run(app).await
}
