use crate::errors::ForumError;
use ws_core::types::SqsNotificationEvent;
use aws_sdk_sqs::Client;
use crate::ForumEvent;

#[derive(Clone)]
pub struct SqsClient {
    client: Client,
    queue_url_noti: String,
    queue_url_cm_del: String
}

impl SqsClient {
    pub fn new(client: Client, queue_url_noti: String, queue_url_cm_del: String) -> Self {
        Self { client, queue_url_noti, queue_url_cm_del }
    }

    pub async fn publish_sqs_event_notification(
        &self,
        event: &SqsNotificationEvent,
    ) -> Result<(), ForumError> {
        let body = serde_json::to_string(event)?;

        self.client
            .send_message()
            .queue_url(&self.queue_url_noti)
            .message_body(body)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }

    pub async fn publish_sqs_event(
        &self,
        event: &ForumEvent,
    ) -> Result<(), ForumError> {
        let body = serde_json::to_string(event)?;

        self.client
            .send_message()
            .queue_url(&self.queue_url_cm_del)
            .message_body(body)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }
    
    
}
