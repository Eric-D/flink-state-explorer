//! Decoders for Apache Kafka connector state (flink-connector-kafka), verified against the
//! connector source (https://github.com/apache/flink-connector-kafka) and real savepoints.
//!
//! Operator-state elements are framed as `[int outerLen][int version][int innerLen][payload]`,
//! where the payload is the connector's `SimpleVersionedSerializer` output.

use crate::parser::java_deser::JavaReader;

/// Unwrap the `[outerLen][version][innerLen][payload]` framing of a Kafka operator-state element.
fn read_versioned(elem: &[u8]) -> Option<(i32, Vec<u8>)> {
    let mut r = JavaReader::new(elem);
    let _outer_len = r.read_int().ok()?;
    let version = r.read_int().ok()?;
    let inner_len = r.read_int().ok()?;
    if inner_len < 0 || inner_len as usize > elem.len() {
        return None;
    }
    let payload = r.read_bytes(inner_len as usize).ok()?;
    Some((version, payload))
}

fn fmt_start_offset(o: i64) -> String {
    match o {
        -3 => "COMMITTED".to_string(),
        -2 => "EARLIEST".to_string(),
        -1 => "LATEST".to_string(),
        _ => o.to_string(),
    }
}

/// `SourceReaderState` element → a `KafkaPartitionSplit`
/// (`writeUTF(topic) + int(partition) + long(startingOffset) + long(stoppingOffset)`).
pub fn decode_source_reader_split(elem: &[u8]) -> Option<String> {
    let (_v, payload) = read_versioned(elem)?;
    let mut r = JavaReader::new(payload.as_slice());
    let topic = r.read_utf().ok()?;
    if topic.is_empty() || topic.len() > 250 {
        return None;
    }
    let partition = r.read_int().ok()?;
    let start = r.read_long().ok()?;
    let stop = r.read_long().ok()?;
    let stop_s = if stop == i64::MIN {
        "none".to_string()
    } else {
        stop.to_string()
    };
    Some(format!(
        "{}-{} [start={}, stop={}]",
        topic,
        partition,
        fmt_start_offset(start),
        stop_s
    ))
}

/// `writer_raw_states` element → a `KafkaWriterState` (starts with `writeUTF(transactionalIdPrefix)`).
pub fn decode_writer_state(elem: &[u8]) -> Option<String> {
    let (_v, payload) = read_versioned(elem)?;
    let mut r = JavaReader::new(payload.as_slice());
    let prefix = r.read_utf().ok()?;
    if prefix.is_empty() || prefix.len() > 250 {
        return None;
    }
    Some(format!("transactionalIdPrefix={}", prefix))
}

/// A pending `KafkaCommittable`, as serialized by the connector's `KafkaCommittableSerializer`
/// (`writeShort(epoch) + writeLong(producerId) + writeUTF(transactionalId)`).
#[derive(Debug, Clone, PartialEq)]
pub struct KafkaCommittable {
    pub epoch: i16,
    pub producer_id: i64,
    pub transactional_id: String,
}

/// `streaming_committer_raw_states` element → a `KafkaCommittable` if a pending transaction is
/// present.
///
/// The Sink-V2 committer wraps each committable via `SimpleVersionedSerialization`
/// (`[int version][int length][payload]`) inside the `CommittableCollectorSerializer` framing;
/// `KafkaCommittableSerializer.getVersion() == 1`. We locate that framing and decode the payload
/// precisely; if none is present the savepoint was taken with no in-flight transaction.
pub fn decode_committer_state(elem: &[u8]) -> Option<String> {
    let (_v, payload) = read_versioned(elem)?;
    match find_kafka_committable(&payload) {
        Some(c) => Some(format!(
            "pending transaction: {} (producerId={}, epoch={})",
            c.transactional_id, c.producer_id, c.epoch
        )),
        None => Some(format!(
            "committer state ({} bytes, no pending transaction)",
            payload.len()
        )),
    }
}

/// Decode a `KafkaCommittable` payload: `short epoch + long producerId + UTF transactionalId`.
fn parse_kafka_committable(blob: &[u8]) -> Option<KafkaCommittable> {
    let mut r = JavaReader::new(blob);
    let epoch = r.read_short().ok()?;
    let producer_id = r.read_long().ok()?;
    let transactional_id = r.read_utf().ok()?;
    // The UTF must consume the blob exactly and look like a Kafka transactional id.
    if r.position() as usize != blob.len() {
        return None;
    }
    if transactional_id.is_empty()
        || transactional_id.len() > 250
        || !transactional_id.contains('-')
        || !transactional_id.bytes().all(|b| (0x20..0x7f).contains(&b))
    {
        return None;
    }
    Some(KafkaCommittable {
        epoch,
        producer_id,
        transactional_id,
    })
}

/// Scan for the `[int version=1][int length][KafkaCommittable]` `SimpleVersionedSerialization`
/// framing inside the committer-collector bytes and decode the first valid committable.
fn find_kafka_committable(bytes: &[u8]) -> Option<KafkaCommittable> {
    let mut i = 0usize;
    while i + 8 <= bytes.len() {
        let version = i32::from_be_bytes(bytes[i..i + 4].try_into().unwrap());
        let len = i32::from_be_bytes(bytes[i + 4..i + 8].try_into().unwrap());
        if version == 1 && (12..=512).contains(&len) {
            let start = i + 8;
            let end = start + len as usize;
            if end <= bytes.len() {
                if let Some(c) = parse_kafka_committable(&bytes[start..end]) {
                    return Some(c);
                }
            }
        }
        i += 1;
    }
    None
}

/// Source enumerator state, stored as the operator **coordinator state**:
/// `[int coordVersion][int enumVersion][int enumLen][enumState]`, where `enumState` =
/// `int(count)` + `count × (writeUTF(topic) + int(partition) + int(assignmentStatus))` +
/// `boolean(initialDiscoveryFinished)`.
pub fn decode_enumerator_state(coord: &[u8]) -> Option<String> {
    let mut r = JavaReader::new(coord);
    let _coord_version = r.read_int().ok()?;
    let _enum_version = r.read_int().ok()?;
    let enum_len = r.read_int().ok()?;
    if enum_len < 0 || enum_len as usize > coord.len() {
        return None;
    }
    let es = r.read_bytes(enum_len as usize).ok()?;
    let mut er = JavaReader::new(es.as_slice());
    let count = er.read_int().ok()?;
    if !(0..=100_000).contains(&count) {
        return None;
    }
    let mut parts = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let topic = er.read_utf().ok()?;
        if topic.is_empty() || topic.len() > 250 {
            return None;
        }
        let partition = er.read_int().ok()?;
        let _status = er.read_int().ok()?; // assignment status
        parts.push(format!("{}-{}", topic, partition));
    }
    let discovery = er.read_boolean().unwrap_or(false);
    Some(format!(
        "assigned: [{}], initialDiscoveryFinished={}",
        parts.join(", "),
        discovery
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `KafkaCommittableSerializer`: `writeShort(epoch) + writeLong(producerId) + writeUTF(id)`.
    fn committable_blob(epoch: i16, producer_id: i64, txn: &str) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&epoch.to_be_bytes());
        v.extend_from_slice(&producer_id.to_be_bytes());
        v.extend_from_slice(&(txn.len() as u16).to_be_bytes());
        v.extend_from_slice(txn.as_bytes());
        v
    }

    #[test]
    fn parses_a_committable_blob() {
        let blob = committable_blob(0, 14, "explorer-txn-1-7");
        assert_eq!(blob.len(), 28); // 2 + 8 + 2 + 16, matches the real savepoint
        let c = parse_kafka_committable(&blob).unwrap();
        assert_eq!(c.epoch, 0);
        assert_eq!(c.producer_id, 14);
        assert_eq!(c.transactional_id, "explorer-txn-1-7");
    }

    #[test]
    fn finds_committable_inside_collector_framing() {
        // SimpleVersionedSerialization framing [int version=1][int len][payload] embedded in noise.
        let blob = committable_blob(3, 4242, "explorer-txn-0-9");
        let mut bytes = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x60];
        bytes.extend_from_slice(&1i32.to_be_bytes()); // version
        bytes.extend_from_slice(&(blob.len() as i32).to_be_bytes());
        bytes.extend_from_slice(&blob);
        bytes.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 0]);
        let c = find_kafka_committable(&bytes).unwrap();
        assert_eq!(c.producer_id, 4242);
        assert_eq!(c.epoch, 3);
        assert_eq!(c.transactional_id, "explorer-txn-0-9");
    }

    #[test]
    fn rejects_committer_state_without_a_transaction() {
        // 64 zero bytes (the "no pending transaction" subtask) yield nothing.
        assert!(find_kafka_committable(&[0u8; 64]).is_none());
    }
}
