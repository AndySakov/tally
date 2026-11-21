pub const MAX_KEY_LEN: usize = 1024;
pub const MAX_VALUE_LEN: usize = 16 * 1024 * 1024;

const _: () = assert!(
    MAX_KEY_LEN <= u16::MAX as usize,
    "MAX_KEY_LEN must fit the u16 key_len field"
);
const _: () = assert!(
    MAX_VALUE_LEN <= u32::MAX as usize,
    "MAX_VALUE_LEN must fit the u32 value_len field"
);
