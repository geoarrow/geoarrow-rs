use std::sync::Arc;

use futures::StreamExt;
use glob::{self, Pattern};
use object_store::{ObjectMeta, ObjectStore};

use crate::error::Result;

/// Find all files within an object store with the specified pattern and suffix
pub async fn expand_glob(
    store: Arc<dyn ObjectStore>,
    pattern: &Pattern,
    suffix: Option<&str>,
) -> Result<Vec<ObjectMeta>> {
    let mut out = Vec::new();

    // There are glob characters in the pattern
    if let Some(first_glob_char_idx) = pattern.as_str().find(['?', '*', '[']) {
        dbg!("glob branch");

        // Strip off the chars before the glob char
        // If there's a / char before the glob char, we use that as the prefix for listing in the
        // object store
        if let Some((prefix, _suffix)) = pattern.as_str()[..first_glob_char_idx].rsplit_once('/') {
            while let Some(item) = store.list(Some(&prefix.into())).next().await {
                let item = item?;
                if item_matches(&item, pattern, suffix) {
                    out.push(item);
                }
            }

            return Ok(out);
        } else {
            dbg!("branch 2");
            while let Some(item) = store.list(None).next().await {
                dbg!("item");
                dbg!(&item);
                let item = item?;
                if item_matches(&item, pattern, suffix) {
                    out.push(item);
                }
            }

            return Ok(out);
        }
    } else {
        // Otherwise, list without a prefix
        while let Some(item) = store.list(None).next().await {
            let item = item?;
            if item_matches(&item, pattern, suffix) {
                out.push(item);
            }
        }
    }

    Ok(out)
}

fn item_matches(item: &ObjectMeta, pattern: &Pattern, suffix: Option<&str>) -> bool {
    if pattern.matches(item.location.as_ref()) {
        if let Some(suffix) = suffix {
            if item.location.as_ref().ends_with(suffix) {
                return true;
            }
        } else {
            return true;
        }
    };

    false
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    #[tokio::test]
    async fn test_matches_path() {
        dbg!("hello world");

        let store = Arc::new(InMemory::new());
        store
            .put(&"file1.txt".into(), PutPayload::new())
            .await
            .unwrap();
        store
            .put(&"file2.txt".into(), PutPayload::new())
            .await
            .unwrap();
        store
            .put(&"file3.txt".into(), PutPayload::new())
            .await
            .unwrap();

        dbg!("done put");

        let list = store.as_ref().list(None);
        let x = list.try_collect::<Vec<_>>().await.unwrap();
        dbg!(x);

        let pattern = Pattern::new("file*.txt").unwrap();
        let result = expand_glob(store, &pattern, Some(".txt")).await.unwrap();

        // result.iter().map(||)
        dbg!(result);
    }
}
