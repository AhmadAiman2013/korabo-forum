use crate::ResponseError;

pub async fn get_parameter(
    ssm_client: &aws_sdk_ssm::Client,
    secret_name: &str,
) -> Result<Vec<String>, ResponseError> {
    let resp = ssm_client
        .get_parameter()
        .name(secret_name)
        .with_decryption(false)
        .send()
        .await?;

    if let Some(parameter) = resp.parameter {
        if let Some(value) = parameter.value {
            // Split a StringList (comma-separated) into a Vec<String>, trimming whitespace
            let items = value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<String>>();

            Ok(items)
        } else {
            Err(ResponseError::NotFound(format!(
                "Value not found for parameter: {}",
                secret_name
            )))
        }
    } else {
        Err(ResponseError::NotFound(format!(
            "Parameter not found: {}",
            secret_name
        )))
    }
}

pub async fn get_parameter_secret(
    ssm_client: &aws_sdk_ssm::Client,
    secret_name: &str,
) -> Result<String, ResponseError> {
    let resp = ssm_client
        .get_parameter()
        .name(secret_name)
        .with_decryption(true)
        .send()
        .await?;

    if let Some(parameter) = resp.parameter {
        if let Some(value) = parameter.value {
            Ok(value)
        } else {
            Err(ResponseError::NotFound(format!(
                "Value not found for parameter: {}",
                secret_name
            )))
        }
    } else {
        Err(ResponseError::NotFound(format!(
            "Parameter not found: {}",
            secret_name
        )))
    }
}
