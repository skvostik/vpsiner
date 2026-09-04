//! In-memory write-through cache over the `services` dictionary in `metadata.db`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::AppResult;
use crate::metadata::MetadataStore;
use crate::model::service_id::ServiceId;
use crate::model::time::TimestampMs;

/// How stale a service's `last_seen_ms` may get before another UPDATE is issued.
const TOUCH_INTERVAL_MS: i64 = 60 * 60 * 1_000;

fn now_ms() -> TimestampMs {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Default)]
struct Inner {
    by_name: HashMap<Arc<str>, ServiceId>,
    by_id: HashMap<ServiceId, Arc<str>>,
    touched_ms: HashMap<ServiceId, i64>,
}

pub struct ServiceRegistry {
    store: Arc<dyn MetadataStore>,
    inner: RwLock<Inner>,
}

impl ServiceRegistry {
    /// Upper bound on how far `last_seen_ms` can trail a service's real activity.
    pub const MAX_WATERMARK_LAG_MS: i64 = TOUCH_INTERVAL_MS;

    /// Preloads the whole dictionary so `name` lookups never touch SQLite.
    pub async fn load(store: Arc<dyn MetadataStore>) -> AppResult<Self> {
        let mut inner = Inner::default();
        for (sid, name) in store.list_services().await? {
            let name: Arc<str> = Arc::from(name);
            inner.by_name.insert(Arc::clone(&name), sid);
            inner.by_id.insert(sid, name);
        }
        tracing::info!(services = inner.by_id.len(), "loaded service dictionary");
        Ok(Self {
            store,
            inner: RwLock::new(inner),
        })
    }

    /// Interns `name`, inserting it into the dictionary if it is new.
    pub async fn id_of(&self, name: &str) -> AppResult<ServiceId> {
        let now = now_ms();
        if let Some(sid) = self.cached_id(name) {
            self.touch(sid, now).await?;
            return Ok(sid);
        }

        // Racing callers converge: `resolve_service` upserts on the UNIQUE name.
        let sid = self.store.resolve_service(name, now).await?;
        let mut inner = self.write();
        let name: Arc<str> = Arc::from(name);
        inner.by_name.insert(Arc::clone(&name), sid);
        inner.by_id.insert(sid, name);
        inner.touched_ms.insert(sid, now);
        Ok(sid)
    }

    pub fn name(&self, sid: ServiceId) -> Option<Arc<str>> {
        self.read().by_id.get(&sid).cloned()
    }

    /// Drops ids that retention has removed from the dictionary.
    pub fn forget(&self, sids: &[ServiceId]) {
        if sids.is_empty() {
            return;
        }
        let mut inner = self.write();
        for sid in sids {
            if let Some(name) = inner.by_id.remove(sid) {
                inner.by_name.remove(&name);
            }
            inner.touched_ms.remove(sid);
        }
    }

    fn cached_id(&self, name: &str) -> Option<ServiceId> {
        self.read().by_name.get(name).copied()
    }

    /// Debounced so a hot ingestion path costs at most one UPDATE per service per interval.
    async fn touch(&self, sid: ServiceId, now: i64) -> AppResult<()> {
        {
            let mut inner = self.write();
            match inner.touched_ms.get(&sid) {
                Some(last) if now - *last < TOUCH_INTERVAL_MS => return Ok(()),
                _ => inner.touched_ms.insert(sid, now),
            };
        }
        self.store.touch_service(sid, now).await
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(|error| error.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Inner> {
        self.inner
            .write()
            .unwrap_or_else(|error| error.into_inner())
    }
}

#[cfg(test)]
impl ServiceRegistry {
    /// Prepopulated registry whose backing store is never touched, for tests that only
    /// need name/id resolution.
    pub fn fixture(names: &[&str]) -> Arc<Self> {
        let mut inner = Inner::default();
        for (index, name) in names.iter().enumerate() {
            let sid = ServiceId::from_u32(index as u32 + 1);
            let name: Arc<str> = Arc::from(*name);
            inner.by_name.insert(Arc::clone(&name), sid);
            inner.by_id.insert(sid, name);
            inner.touched_ms.insert(sid, i64::MAX);
        }
        Arc::new(Self {
            store: Arc::new(crate::metadata::MockMetadataStore::new()),
            inner: RwLock::new(inner),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MockMetadataStore, SqliteMetadataStore};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    async fn sqlite_registry(name: &str) -> ServiceRegistry {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("vpsiner-registry-{name}-{suffix}.db"));
        let store = SqliteMetadataStore::connect(path, 1_024, Duration::from_secs(5))
            .await
            .unwrap();
        ServiceRegistry::load(Arc::new(store)).await.unwrap()
    }

    #[tokio::test]
    async fn interns_once_and_resolves_both_ways() {
        let registry = sqlite_registry("intern").await;

        let sid = registry.id_of("shop-web").await.unwrap();

        assert_eq!(registry.id_of("shop-web").await.unwrap(), sid);
        assert_eq!(registry.name(sid).as_deref(), Some("shop-web"));
    }

    #[tokio::test]
    async fn preloads_the_dictionary_from_disk() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("vpsiner-registry-preload-{suffix}.db"));

        let store = Arc::new(
            SqliteMetadataStore::connect(&path, 1_024, Duration::from_secs(5))
                .await
                .unwrap(),
        );
        let sid = ServiceRegistry::load(store.clone())
            .await
            .unwrap()
            .id_of("shop-web")
            .await
            .unwrap();

        let reloaded = ServiceRegistry::load(store).await.unwrap();
        assert_eq!(reloaded.name(sid).as_deref(), Some("shop-web"));
    }

    #[tokio::test]
    async fn forget_drops_both_directions() {
        let registry = sqlite_registry("forget").await;
        let sid = registry.id_of("gone").await.unwrap();

        registry.forget(&[sid]);

        assert_eq!(registry.name(sid), None);
    }

    #[tokio::test]
    async fn repeated_lookups_do_not_hammer_the_store() {
        let mut store = MockMetadataStore::new();
        store.expect_list_services().returning(|| Ok(Vec::new()));
        store
            .expect_resolve_service()
            .times(1)
            .returning(|_, _| Ok(ServiceId::from_u32(7)));
        store.expect_touch_service().never();

        let registry = ServiceRegistry::load(Arc::new(store)).await.unwrap();
        for _ in 0..100 {
            assert_eq!(registry.id_of("svc").await.unwrap(), ServiceId::from_u32(7));
        }
    }
}
