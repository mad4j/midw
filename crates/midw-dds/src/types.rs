/// DDS message carrying a node command with a correlation id.
#[derive(Debug, Clone, hdds::DDS)]
pub struct NodeRequest {
    pub correlation_id: u64,
    pub payload: Vec<u8>,
}

/// DDS message carrying a node response with a matching correlation id.
#[derive(Debug, Clone, hdds::DDS)]
pub struct NodeReply {
    pub correlation_id: u64,
    pub payload: Vec<u8>,
}
