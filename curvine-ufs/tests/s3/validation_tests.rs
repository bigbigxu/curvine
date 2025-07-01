// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::s3::test_utils::get_s3_test_config;
use curvine_ufs::fs::aws_utils::{validate_s3_config, S3ConfigValidationResult};

#[test]
fn test_s3_config_validation() {
    // Effective configuration test
    let valid_config = get_s3_test_config();
    let result = validate_s3_config(&valid_config);
    println!("{:?}", result.error_message());
    assert!(result.is_valid());

    // Missing region_name test
    let mut missing_region = valid_config.clone();
    missing_region.remove("s3.region_name");
    let result = validate_s3_config(&missing_region);
    assert!(
        matches!(result, S3ConfigValidationResult::MissingRequiredParam(param) if param == "s3.region_name")
    );

    // Missing bucket_name test
    let mut missing_bucket = valid_config.clone();
    missing_bucket.remove("s3.bucket_name");
    let result = validate_s3_config(&missing_bucket);
    assert!(
        matches!(result, S3ConfigValidationResult::MissingRequiredParam(param) if param == "s3.bucket_name")
    );

    // Invalid endpoint_url test
    let mut invalid_endpoint = valid_config.clone();
    invalid_endpoint.insert("s3.endpoint_url".to_string(), "invalid-url".to_string());
    let result = validate_s3_config(&invalid_endpoint);
    assert!(
        matches!(result, S3ConfigValidationResult::InvalidParamFormat { param_name, reason } 
                    if param_name == "s3.endpoint_url" && reason.contains("http://"))
    );

    // Empty endpoint_url test
    let mut empty_endpoint = valid_config.clone();
    empty_endpoint.insert("s3.endpoint_url".to_string(), "".to_string());
    let result = validate_s3_config(&empty_endpoint);
    assert!(
        matches!(result, S3ConfigValidationResult::InvalidParamFormat { param_name, reason } 
                    if param_name == "s3.endpoint_url" && reason.contains("Can't be empty"))
    );

    // Incomplete certification information test -only access without secret
    let mut incomplete_creds1 = valid_config.clone();
    incomplete_creds1.insert(
        "s3.credentials.access".to_string(),
        "test-access".to_string(),
    );
    incomplete_creds1.remove("s3.credentials.secret");
    let result = validate_s3_config(&incomplete_creds1);
    assert!(
        matches!(result, S3ConfigValidationResult::IncompleteCredentials(msg) 
                    if msg.contains("s3.credentials.secret"))
    );

    // Incomplete certification information test -only secret without access
    let mut incomplete_creds2 = valid_config.clone();
    incomplete_creds2.insert(
        "s3.credentials.secret".to_string(),
        "test-secret".to_string(),
    );
    incomplete_creds2.remove("s3.credentials.access");
    let result = validate_s3_config(&incomplete_creds2);
    assert!(
        matches!(result, S3ConfigValidationResult::IncompleteCredentials(msg) 
                    if msg.contains("s3.credentials.access"))
    );

    // Empty certification information test
    let mut empty_creds = valid_config.clone();
    empty_creds.insert("s3.credentials.access".to_string(), "".to_string());
    empty_creds.insert(
        "s3.credentials.secret".to_string(),
        "test-secret".to_string(),
    );
    let result = validate_s3_config(&empty_creds);
    assert!(
        matches!(result, S3ConfigValidationResult::InvalidParamFormat { param_name, reason } 
                    if param_name == "s3.credentials.access" && reason.contains("Can't be empty"))
    );

    let mut empty_secret = valid_config.clone();
    empty_secret.insert(
        "s3.credentials.access".to_string(),
        "test-access".to_string(),
    );
    empty_secret.insert("s3.credentials.secret".to_string(), "".to_string());
    let result = validate_s3_config(&empty_secret);
    assert!(
        matches!(result, S3ConfigValidationResult::InvalidParamFormat { param_name, reason } 
                    if param_name == "s3.credentials.secret" && reason.contains("Can't be empty"))
    );
}

#[test]
fn test_error_messages() {
    //Test the error message format
    //Error message missing necessary parameters
    let missing_param =
        S3ConfigValidationResult::MissingRequiredParam("s3.region_name".to_string());
    assert_eq!(
        missing_param.error_message(),
        Some("Missing the necessary s3 parameters: s3.region_name".to_string())
    );

    // Error message with wrong parameter format
    let invalid_format = S3ConfigValidationResult::InvalidParamFormat {
        param_name: "s3.endpoint_url".to_string(),
        reason: "Must start with http://or https://".to_string(),
    };
    assert_eq!(
        invalid_format.error_message(),
        Some(
            "S3 parameter format error - s3.endpoint_url: Must start with http://or https://"
                .to_string()
        )
    );

    // Error message with incomplete authentication information
    let incomplete_creds = S3ConfigValidationResult::IncompleteCredentials(
        "Access is provided but s3.credentials.secret is missing".to_string(),
    );
    assert_eq!(
        incomplete_creds.error_message(),
        Some("S3 certification information is incomplete: Access is provided but s3.credentials.secret is missing".to_string())
    );

    // No error message when configured
    assert_eq!(S3ConfigValidationResult::Valid.error_message(), None);
}
