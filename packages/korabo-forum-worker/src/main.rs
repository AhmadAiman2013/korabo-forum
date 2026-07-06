use aws_config::BehaviorVersion;
use aws_lambda_events::sqs::SqsEvent;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_s3::Client as S3Client;
use chrono::Utc;
use forum_core::{ForumError, ForumEvent, ForumRepository, S3Store, SqsClient, get_parameter};
use group_core::{DynamoDBError, GroupsRepository};
use lambda_runtime::tracing::{error, init_default_subscriber};
use lambda_runtime::{Error, LambdaEvent, run, service_fn};
use serde_json::{from_str, json};
use std::sync::Arc;
use uuid::Uuid;
use ws_core::types::{NotificationTargeting, SqsNotificationEvent};

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<ForumRepository>,
    pub group_repo: Arc<GroupsRepository>,
    pub members_table: String,
    pub s3: S3Store,
    pub queue: SqsClient,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_default_subscriber();

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = DynamoDbClient::new(&config);
    let forums_table = String::from("korabo_forum");
    let members_table = String::from("korabo_group_members");

    let repo = Arc::new(ForumRepository::new(
        client.clone(),
        forums_table,
        members_table.clone(),
    ));
    let group_repo = Arc::new(GroupsRepository::new(client.clone()));

    let ssm_client = aws_sdk_ssm::Client::new(&config);

    let (ssm_value, ssm_value_2) = tokio::join!(
        get_parameter(&ssm_client, "/korabo/prod/sqs/forum"),
        get_parameter(&ssm_client, "/korabo/prod/sqs/forum-queue")
    );

    let ssm_value = ssm_value?;
    let bucket = ssm_value
        .first()
        .cloned()
        .expect("S3 bucket not found in SSM parameter");

    let sqs = aws_sdk_sqs::Client::new(&config);
    let ssm_value_2 = ssm_value_2?;
    let queue_url_noti = ssm_value_2
        .first()
        .cloned()
        .expect("Noti queue not found in SSM parameter");
    let queue_url_cm_del = ssm_value_2
        .last()
        .cloned()
        .expect("Forum queue URL not found in SSM parameter");
    let queue = SqsClient::new(sqs, queue_url_noti, queue_url_cm_del);

    let s3_client = S3Client::new(&config);
    let s3 = S3Store::new(s3_client, bucket);

    let state = AppState {
        repo,
        group_repo,
        members_table,
        s3,
        queue,
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

        let noti_event: ForumEvent = match from_str(&body) {
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

async fn process_event(evt: &ForumEvent, state: &AppState) -> Result<(), ForumError> {
    match evt {
        ForumEvent::PostCreated {
            post_id,
            group_id,
            author_id,
            title,
        } => {
            let members_id =
                get_all_members_id(&state.group_repo, state.members_table.clone(), group_id)
                    .await
                    .map_err(|e| ForumError::General(e.to_string()))?;

            let payload = json!({
                "post_id": post_id,
                "title": title,
            });

            let event = SqsNotificationEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "PostCreated".to_string(),
                actor_id: author_id.clone(),
                targeting: NotificationTargeting {
                    user_ids: members_id,
                    group_id: Some(group_id.clone()),
                    exclude_user_ids: vec![author_id.to_owned()].into(),
                },
                payload: serde_json::to_value(payload)?,
                created_at: Utc::now().to_rfc3339(),
            };

            state.queue.publish_sqs_event_notification(&event).await?;
        }
        ForumEvent::PostUpdated {
            post_id,
            group_id,
            author_id,
            title,
        } => {
            let members_id =
                get_all_members_id(&state.group_repo, state.members_table.clone(), group_id)
                    .await
                    .map_err(|e| ForumError::General(e.to_string()))?;

            let payload = json!({
                "post_id": post_id,
                "title": title,
            });

            let event = SqsNotificationEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "PostUpdated".to_string(),
                actor_id: author_id.clone(),
                targeting: NotificationTargeting {
                    user_ids: members_id,
                    group_id: Some(group_id.clone()),
                    exclude_user_ids: vec![author_id.to_owned()].into(),
                },
                payload: serde_json::to_value(payload)?,
                created_at: Utc::now().to_rfc3339(),
            };

            state.queue.publish_sqs_event_notification(&event).await?;
        }
        ForumEvent::CommentCreated {
            comment_id,
            post_id,
            group_id,
            author_id,
        } => {
            let members_id =
                get_all_members_id(&state.group_repo, state.members_table.clone(), group_id)
                    .await
                    .map_err(|e| ForumError::General(e.to_string()))?;

            let payload = json!({
                "comment_id": comment_id,
                "post_id": post_id,
            });

            let event = SqsNotificationEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "CommentCreated".to_string(),
                actor_id: author_id.clone(),
                targeting: NotificationTargeting {
                    user_ids: members_id,
                    group_id: Some(group_id.clone()),
                    exclude_user_ids: vec![author_id.to_owned()].into(),
                },
                payload: serde_json::to_value(payload)?,
                created_at: Utc::now().to_rfc3339(),
            };

            state.queue.publish_sqs_event_notification(&event).await?;
        }
        ForumEvent::CommentUpdated {
            comment_id,
            post_id,
            group_id,
            author_id,
        } => {
            let members_id =
                get_all_members_id(&state.group_repo, state.members_table.clone(), group_id)
                    .await
                    .map_err(|e| ForumError::General(e.to_string()))?;

            let payload = json!({
                "comment_id": comment_id,
                "post_id": post_id,
            });

            let event = SqsNotificationEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "CommentUpdated".to_string(),
                actor_id: author_id.clone(),
                targeting: NotificationTargeting {
                    user_ids: members_id,
                    group_id: Some(group_id.clone()),
                    exclude_user_ids: vec![author_id.to_owned()].into(),
                },
                payload: serde_json::to_value(payload)?,
                created_at: Utc::now().to_rfc3339(),
            };

            state.queue.publish_sqs_event_notification(&event).await?;
        }
        ForumEvent::PostDeleted {
            post_id,
            post_attachments,
        } => {
            // 1. get all comments for deletion
            let comments = state.repo.get_all_comments_for_deletion(post_id).await?;

            // 2. get comments Sks
            let comment_sks: Vec<String> = comments.iter().map(|(sk, _)| sk.clone()).collect();

            // 3. get attachment keys for deletion
            let mut all_attachments = post_attachments.to_owned();
            all_attachments.extend(
                comments
                    .into_iter()
                    .flat_map(|(_, attachments)| attachments),
            );
            let attachment_keys: Vec<String> =
                all_attachments.iter().map(|a| a.key.clone()).collect();

            // 4. delete all items
            state
                .repo
                .finalize_post_deletion(post_id, &comment_sks)
                .await?;

            // 5. delete all keys in s3 bucket
            if !attachment_keys.is_empty() {
                state.s3.delete(&attachment_keys).await?;
            }
        }
        ForumEvent::AttachmentsDeleted {
            deleted_attachments,
        } => state.s3.delete(deleted_attachments).await?,
    }
    Ok(())
}

async fn get_all_members_id(
    repo: &GroupsRepository,
    members_table: String,
    group_id: &str,
) -> Result<Vec<String>, DynamoDBError> {
    let members = repo.list_group_members(&members_table, group_id).await?;
    let members_id = members.iter().map(|x| x.user_id.clone()).collect();
    Ok(members_id)
}
