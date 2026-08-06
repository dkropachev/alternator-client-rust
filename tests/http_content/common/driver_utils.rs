// Copyright ScyllaDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use aws_sdk_dynamodb::client::Waiters;
use std::time::Duration;

pub async fn delete_table_cleanup(client: &aws_sdk_dynamodb::Client, table_name: &str) {
    // try to delete
    let mut result = client.delete_table().table_name(table_name).send().await;

    // wait if resource is not yet ready
    if let Err(ref e) = result
        && e.as_service_error()
            .is_some_and(|s| s.is_resource_in_use_exception())
    {
        client
            .wait_until_table_exists()
            .table_name(table_name)
            .wait(Duration::from_secs(5))
            .await
            .unwrap();

        result = client.delete_table().table_name(table_name).send().await;
    }

    // final check
    if let Err(e) = result
        && !e
            .as_service_error()
            .is_some_and(|s| s.is_resource_not_found_exception())
    {
        panic!("Cleanup failed: {e:?}");
    }
}
