//! The watch path: a kube-rs watch on `PgInstance.status.primaryEpoch`.
//!
//! Sub-second in the happy path and useless under partition, which is why it is
//! one of three and not the answer on its own. It is **control plane only**: it
//! runs in its own task, it touches nothing on the data path but
//! [`EpochFence::observe`], and a proxy whose API server has gone away keeps
//! fencing through the verify path without noticing.
//!
//! `DynamicObject` rather than a generated Rust mirror of the CRD: the schema
//! lives in Go, and a second hand-maintained copy of it here is a place for the
//! two to disagree about the one field that matters.

use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt as _;
use kube::api::{Api, DynamicObject, GroupVersionKind};
use kube::discovery::ApiResource;
use kube::runtime::watcher;
use tracing::{debug, info, warn};

use super::{Epoch, EpochFence, EpochSource};

/// The `PgInstance` GVK, spelled to match `api/v1alpha1/groupversion_info.go`.
pub const GROUP: &str = "pgelastic.io";
pub const VERSION: &str = "v1alpha1";
pub const KIND: &str = "PgInstance";

/// How long a failed watch waits before it is rebuilt.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// Which `PgInstance` this proxy fronts.
#[derive(Debug, Clone)]
pub struct WatchTarget {
    pub namespace: String,
    pub name: String,
}

/// Reads `status.primaryEpoch` off a watched object.
///
/// A missing or non-integral field yields `None` rather than zero: an object
/// whose status has not been written yet is absent evidence, and treating it as
/// epoch zero would make the first real reading look like an advance from a
/// number nobody published.
pub fn epoch_of(object: &DynamicObject) -> Option<Epoch> {
    let value = object.data.get("status")?.get("primaryEpoch")?;
    let epoch = value.as_u64().or_else(|| {
        // The CRD types it `int64`, and a negative epoch is not a smaller
        // epoch: it is a value this proxy refuses to act on.
        value.as_i64().and_then(|signed| u64::try_from(signed).ok())
    })?;
    Some(Epoch::new(epoch))
}

/// Runs the watch until `shutdown` goes true, rebuilding it across failures.
///
/// Never returns an error: a watch that cannot be established is a degraded
/// control plane, not a reason to stop proxying. The proxy keeps fencing from
/// the verify path, which is the whole reason that path is mandatory.
pub async fn run(
    target: WatchTarget,
    fence: Arc<EpochFence>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        let attempt = tokio::select! {
            result = watch_once(&target, &fence) => result,
            () = wait_true(&mut shutdown) => return,
        };
        if let Err(error) = attempt {
            warn!(
                %error,
                namespace = target.namespace,
                name = target.name,
                "the PgInstance watch failed; the verify path still fences"
            );
        }
        tokio::select! {
            () = tokio::time::sleep(RETRY_BACKOFF) => {}
            () = wait_true(&mut shutdown) => return,
        }
    }
}

async fn watch_once(target: &WatchTarget, fence: &EpochFence) -> Result<(), kube::Error> {
    let client = kube::Client::try_default().await?;
    let resource = ApiResource::from_gvk(&GroupVersionKind::gvk(GROUP, VERSION, KIND));
    let api: Api<DynamicObject> = Api::namespaced_with(client, &target.namespace, &resource);

    info!(
        namespace = target.namespace,
        name = target.name,
        "watching PgInstance.status.primaryEpoch"
    );
    let config = watcher::Config::default().fields(&format!("metadata.name={}", target.name));
    let stream = watcher(api, config);
    futures_util::pin_mut!(stream);

    while let Some(event) = stream.try_next().await.map_err(as_kube_error)? {
        for object in objects(event) {
            if let Some(epoch) = epoch_of(&object) {
                fence.observe(EpochSource::Watch, epoch);
            } else {
                debug!(
                    name = object.metadata.name,
                    "a watched PgInstance carries no primaryEpoch yet"
                );
            }
        }
    }
    Ok(())
}

fn objects(event: watcher::Event<DynamicObject>) -> Vec<DynamicObject> {
    match event {
        watcher::Event::Apply(object) | watcher::Event::InitApply(object) => vec![object],
        // A deleted PgInstance publishes no new epoch, and the last one it
        // published stays in force. Nothing about a deletion lowers the fence.
        watcher::Event::Delete(_) | watcher::Event::Init | watcher::Event::InitDone => Vec::new(),
    }
}

fn as_kube_error(error: watcher::Error) -> kube::Error {
    match error {
        watcher::Error::InitialListFailed(inner)
        | watcher::Error::WatchStartFailed(inner)
        | watcher::Error::WatchFailed(inner) => inner,
        other => kube::Error::Service(Box::new(std::io::Error::other(other.to_string()))),
    }
}

async fn wait_true(rx: &mut tokio::sync::watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(status: &serde_json::Value) -> DynamicObject {
        let resource = ApiResource::from_gvk(&GroupVersionKind::gvk(GROUP, VERSION, KIND));
        DynamicObject::new("instance-a", &resource).data(serde_json::json!({ "status": status }))
    }

    #[test]
    fn the_epoch_is_read_off_the_status_the_operator_publishes() {
        assert_eq!(
            epoch_of(&object(&serde_json::json!({ "primaryEpoch": 12 }))),
            Some(Epoch::new(12))
        );
    }

    #[test]
    fn a_status_without_an_epoch_yet_is_absent_evidence_rather_than_zero() {
        assert_eq!(
            epoch_of(&object(
                &serde_json::json!({ "currentPrimary": "instance-a-1" })
            )),
            None
        );
        let resource = ApiResource::from_gvk(&GroupVersionKind::gvk(GROUP, VERSION, KIND));
        assert_eq!(epoch_of(&DynamicObject::new("bare", &resource)), None);
    }

    #[test]
    fn a_negative_or_non_integral_epoch_is_refused_rather_than_coerced() {
        assert_eq!(
            epoch_of(&object(&serde_json::json!({ "primaryEpoch": -1 }))),
            None
        );
        assert_eq!(
            epoch_of(&object(&serde_json::json!({ "primaryEpoch": "12" }))),
            None
        );
    }

    #[test]
    fn a_deletion_publishes_no_epoch_so_the_fence_cannot_be_lowered_by_one() {
        let deleted = objects(watcher::Event::Delete(object(
            &serde_json::json!({ "primaryEpoch": 1 }),
        )));
        assert!(deleted.is_empty());
    }

    #[test]
    fn an_applied_object_is_the_one_the_fence_is_fed() {
        let applied = objects(watcher::Event::Apply(object(
            &serde_json::json!({ "primaryEpoch": 4 }),
        )));
        assert_eq!(applied.len(), 1);
        assert_eq!(epoch_of(&applied[0]), Some(Epoch::new(4)));
    }

    #[test]
    fn the_watched_kind_matches_the_go_api_group() {
        assert_eq!(GROUP, "pgelastic.io");
        assert_eq!(VERSION, "v1alpha1");
        assert_eq!(KIND, "PgInstance");
    }
}
