use crate::record::Op;
use crate::{Key, Value};

/// Sets and deletes for [`Store::write`](crate::Store::write) to make
/// durable with one fsync and apply all or nothing, in the order added.
#[derive(Debug, Default)]
pub struct Batch {
    pub(crate) ops: Vec<Op>,
}

impl Batch {
    pub fn new() -> Batch {
        Batch::default()
    }

    pub fn set(&mut self, key: Key, value: Value) -> &mut Batch {
        self.ops.push(Op::Set { key, value });
        self
    }

    pub fn delete(&mut self, key: Key) -> &mut Batch {
        self.ops.push(Op::Delete { key });
        self
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}
