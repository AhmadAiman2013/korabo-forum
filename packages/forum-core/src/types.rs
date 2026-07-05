use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Attachment {
    pub key: String,
    pub content_type: String,
    pub size_bytes: i64,
    #[serde(skip_deserializing)]
    pub url: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Post {
    pub post_id: String,
    pub group_id: String,
    pub author_id: String,
    pub title: String,
    pub body: String,
    pub attachments: Vec<Attachment>,
    pub created_at: String,
    pub deleted_at: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Comment {
    pub comment_id: String,
    pub sk: String,
    pub post_id: String,
    pub author_id: String,
    pub body: String,
    pub attachments: Vec<Attachment>,
    pub created_at: String,
}

// ---- Request Object -----
#[derive(Deserialize, Debug)]
pub struct CreatePostRequest {
    pub group_id: String,
    pub title: String,
    pub body: String,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
}

#[derive(Deserialize, Debug)]
pub struct ListPostsRequest {
    pub group_id: String,
    pub cursor: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Deserialize, Debug)]
pub struct CreateCommentRequest {
    pub body: String,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
}

#[derive(Deserialize, Debug)]
pub struct ListCommentsRequest {
    pub group_id: String,
    pub cursor: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Deserialize, Debug)]
pub struct UpdatePostRequest {
    pub title: String,
    pub body: String,
    pub attachments: Vec<Attachment>,
}

#[derive(Deserialize, Debug)]
pub struct UpdateCommentRequest {
    pub body: String,
    pub attachments: Vec<Attachment>,
}

#[derive(Deserialize, Debug)]
pub struct PresignUploadRequest {
    pub group_id: String,
    pub file_name: String,
    pub content_type: String,
    pub content_length: i64,
}


// --- Item Shapes ---
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PostItem {
    #[serde(rename = "PK")]
    pub pk: String,
    #[serde(rename = "SK")]
    pub sk: String,
    #[serde(rename = "GSI1PK")]
    pub gsi1pk: String,
    #[serde(rename = "GSI1SK")]
    pub gsi1sk: String,
    pub post_id: String,
    pub group_id: String,
    pub author_id: String,
    pub title: String,
    pub body: String,
    pub attachments: Vec<Attachment>,
    pub created_at: String,
    #[serde(default)]
    pub deleted: bool,
    pub deleted_at: Option<String>,
}

impl From<PostItem> for Post {
    fn from(item: PostItem) -> Self {
        Post {
            post_id: item.post_id,
            group_id: item.group_id,
            author_id: item.author_id,
            title: item.title,
            body: item.body,
            attachments: item.attachments,
            created_at: item.created_at,
            deleted_at: item.deleted_at,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommentItem {
    #[serde(rename = "PK")]
    pub pk: String,
    #[serde(rename = "SK")]
    pub sk: String,
    pub comment_id: String,
    pub post_id: String,
    pub author_id: String,
    pub body: String,
    pub attachments: Vec<Attachment>,
    pub created_at: String,
}

impl From<CommentItem> for Comment {
    fn from(item: CommentItem) -> Self {
        Comment {
            comment_id: item.comment_id,
            sk: item.sk,
            post_id: item.post_id,
            author_id: item.author_id,
            body: item.body,
            attachments: item.attachments,
            created_at: item.created_at,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ItemKey {
    #[serde(rename = "PK")]
    pub pk: String,
    #[serde(rename = "SK")]
    pub sk: String,
}

#[derive(serde::Deserialize)]
pub struct SkAndAttachments {
    #[serde(rename = "SK")]
    pub sk: String,
    pub attachments: Option<Vec<Attachment>>,
}

#[derive(Serialize, Debug)]
pub struct PresignedUpload {
    pub upload_url: String,
    pub key: String,
    pub expires_in_secs: u64,
}


// --------- SQS Model --------
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum ForumEvent {
    PostCreated {
        post_id: String,
        group_id: String,
        author_id: String,
        title: String,
    },

    PostUpdated {
        post_id: String,
        group_id: String,
        author_id: String,
        title: String,
    },

    CommentCreated {
        comment_id: String,
        post_id: String,
        group_id: String,
        author_id: String,
    },
    
    CommentUpdated {
        comment_id: String,
        post_id: String,
        group_id: String,
        author_id: String,
    }


}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum ForumPostEvent {
    PostDeleted {
        post_id: String,
        post_attachments: Vec<Attachment>,
    },

    AttachmentsDeleted {
        deleted_attachments: Vec<String>,
    }
}