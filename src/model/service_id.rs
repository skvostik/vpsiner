/// A service name interned into a compact id by the metadata dictionary.
///
/// Deliberately not `Serialize`/`Deserialize`: this is a storage detail and must be resolved
/// back to the service name before it reaches the API.
///
/// `Default` is the never-issued id 0 — SQLite `AUTOINCREMENT` starts at 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ServiceId(u32);

impl ServiceId {
    pub fn from_u32(value: u32) -> Self {
        Self(value)
    }

    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for ServiceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
