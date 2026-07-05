use aws_lambda_events::sqs::SqsEvent;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use lambda_runtime::tracing::{error, init_default_subscriber};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::from_str;
use std::sync::Arc;
use aws_config::BehaviorVersion;
use forum_core::{get_parameter, ForumError, ForumPostEvent, ForumRepository, S3Store};

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<ForumRepository>,
    pub s3: S3Store,
}

#[tokio::main]
async fn main () -> Result<(), Error> {
    init_default_subscriber();

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = DynamoDbClient::new(&config);
    let forums_table = String::from("korabo_forum");
    let members_table = String::from("korabo_group_members");

    let repo = Arc::new(ForumRepository::new(client, forums_table, members_table));

    let ssm_client = aws_sdk_ssm::Client::new(&config);

    let ssm_value = get_parameter(&ssm_client, "/korabo/prod/sqs/forum").await?;
    let bucket = ssm_value.first().cloned().expect("S3 bucket not found in SSM parameter");

    let s3_client = S3Client::new(&config);
    let s3 = S3Store::new(s3_client, bucket);

    let state = AppState {
        repo,
        s3
    };

    run(service_fn(move |event| {
        let state = state.clone();
        async move { function_handler(event, state).await }
    }))
        .await

}

pub async fn function_handler(event: LambdaEvent<SqsEvent>, state: AppState) -> Result<(), Error> {
    for record in event.payload.records {
        let body = match record.body {
            Some(b) => b,
            None => {
                error!(
                    "Received SQS record with no body, message_id: {:?}",
                    record.message_id
                );
                continue;
            }
        };

        let noti_event: ForumPostEvent = match from_str(&body) {
            Ok(e) => e,
            Err(err) => {
                error!("Failed to deserialize message: {:?} body: {}", err, body);
                continue;
            }
        };

        if let Err(err) = process_event(&noti_event, &state).await {
            error!(
                "Failed to process message_id {:?}: {}",
                record.message_id, err
            );
            return Err(err.into());
        }
    }
    Ok(())
}

async fn process_event(evt: &ForumPostEvent, state: &AppState) -> Result<(), ForumError> {
    match evt {
        ForumPostEvent::PostDeleted { post_id, post_attachments } => {
            // 1. get all comments for deletion
            let comments = state.repo.get_all_comments_for_deletion(post_id).await?;

            // 2. get comments Sks
            let comment_sks: Vec<String> = comments.iter().map(|(sk, _)| sk.clone()).collect();

            // 3. get attachment keys for deletion
            let mut all_attachments = post_attachments.to_owned();
            all_attachments.extend(comments.into_iter().flat_map(|(_, attachments)| attachments));
            let attachment_keys: Vec<String> = all_attachments.iter().map(|a| a.key.clone()).collect();

            // 4. delete all items
            state.repo.finalize_post_deletion(post_id, &comment_sks).await?;

            // 5. delete all keys in s3 bucket
            if !attachment_keys.is_empty() {
                state.s3.delete(&attachment_keys).await?;
            }
        },
        ForumPostEvent::AttachmentsDeleted { deleted_attachments} => {
            state.s3.delete(deleted_attachments).await?
        }
    }
    Ok(())
}

