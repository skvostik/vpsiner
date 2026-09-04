use serde::{Deserialize, Serialize};

/// The first 12 hex chars of a Docker container id, packed into 6 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ContainerId([u8; 6]);

impl ContainerId {
    /// `None` if `full_id` is shorter than 12 chars or isn't lowercase hex.
    pub fn parse(full_id: &str) -> Option<Self> {
        let prefix = full_id.get(..12)?.as_bytes();
        let mut bytes = [0u8; 6];
        for (index, byte) in bytes.iter_mut().enumerate() {
            let high = hex_nibble(prefix[index * 2])?;
            let low = hex_nibble(prefix[index * 2 + 1])?;
            *byte = (high << 4) | low;
        }
        Some(Self(bytes))
    }

    pub fn to_hex(self) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(12);
        for byte in self.0 {
            write!(out, "{byte:02x}").expect("writing to a String never fails");
        }
        out
    }

    pub fn as_bytes(&self) -> &[u8; 6] {
        &self.0
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        <[u8; 6]>::try_from(bytes).ok().map(Self)
    }
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

impl std::fmt::Display for ContainerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for ContainerId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for ContainerId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        ContainerId::parse(&text)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid container id: {text}")))
    }
}

#[cfg(test)]
mod container_id_tests {
    use super::ContainerId;

    #[test]
    fn round_trips_through_hex() {
        let id = ContainerId::parse("8af7d6c1273dextra").expect("valid hex prefix");
        assert_eq!(id.to_hex(), "8af7d6c1273d");
    }

    #[test]
    fn round_trips_through_bytes() {
        let id = ContainerId::parse("8af7d6c1273d").unwrap();
        let bytes = *id.as_bytes();
        assert_eq!(ContainerId::from_bytes(&bytes), Some(id));
    }

    #[test]
    fn rejects_short_input() {
        assert_eq!(ContainerId::parse("abc123"), None);
    }

    #[test]
    fn rejects_non_hex_input() {
        assert_eq!(ContainerId::parse("short-id-1234"), None);
    }
}
