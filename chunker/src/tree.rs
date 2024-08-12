use bytes::Bytes;

#[derive(Debug, PartialEq)]
pub struct Node {
    pub hash: Bytes,
    pub content: NodeContent,
}

#[derive(Debug, PartialEq)]
pub enum NodeContent {
    Leaf { data: Bytes },
    Parent { left: Box<Node>, right: Box<Node> },
}
