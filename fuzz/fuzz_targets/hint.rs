//! Compacts a store so its segment has a hint file, writes more records
//! after it, damages the hint, and reopens.
//!
//! However the hint is damaged (a changed byte, a truncation, or arbitrary
//! bytes in its place), `open` succeeds and every key reads back the value
//! the operations left. A damaged hint is rejected by its checksum and the
//! whole log is read instead. Checksums are never fixed up here: a hint
//! with a valid checksum is trusted by design.

#![no_main]

use std::collections::HashMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tally::{Key, Store, Value};

#[derive(Arbitrary, Debug)]
enum Op {
    Set { key: u8, value: Vec<u8> },
    Delete { key: u8 },
}

#[derive(Arbitrary, Debug)]
enum Damage {
    None,
    ChangeByte { at: u32, mask: u8 },
    Truncate { at: u32 },
    Replace { bytes: Vec<u8> },
}

#[derive(Arbitrary, Debug)]
struct Input {
    before: Vec<Op>,
    after: Vec<Op>,
    damage: Damage,
}

fn key(k: u8) -> Key {
    Key::try_from(&[k][..]).unwrap()
}

fn apply(store: &Store, state: &mut HashMap<u8, Vec<u8>>, ops: Vec<Op>) {
    for op in ops {
        match op {
            Op::Set { key: k, value } => {
                let Ok(v) = Value::try_from(&value[..]) else {
                    continue;
                };
                store.set(key(k), v).unwrap();
                state.insert(k, value);
            }
            Op::Delete { key: k } => {
                store.delete(key(k)).unwrap();
                state.remove(&k);
            }
        }
    }
}

fuzz_target!(|input: Input| {
    let dir = tempfile::tempdir().unwrap();
    let mut state = HashMap::new();
    let store = Store::open(dir.path()).unwrap();
    apply(&store, &mut state, input.before);
    store.compact().unwrap();
    apply(&store, &mut state, input.after);
    drop(store);

    // Compaction leaves one segment with a hint, unless there was nothing
    // to compact.
    let Some(path) = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "hint"))
    else {
        return;
    };
    let mut hint = std::fs::read(&path).unwrap();
    match input.damage {
        Damage::None => {}
        Damage::ChangeByte { at, mask } => {
            if mask == 0 {
                return;
            }
            let i = at as usize % hint.len();
            hint[i] ^= mask;
        }
        Damage::Truncate { at } => hint.truncate(at as usize % hint.len()),
        Damage::Replace { bytes } => hint = bytes,
    }
    std::fs::write(&path, &hint).unwrap();

    let store = Store::open(dir.path()).expect("a damaged hint must not stop open");
    for k in 0..=u8::MAX {
        let got = store.get(&key(k)).expect("every key must read back");
        assert_eq!(
            got.as_ref().map(Value::as_bytes),
            state.get(&k).map(Vec::as_slice),
            "key {k}"
        );
    }
});
