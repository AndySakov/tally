//! `open` logs a warning when it truncates a torn tail. The logger is global
//! to the process, so this test lives alone in its own test binary.

use std::sync::Mutex;

use log::{Level, LevelFilter, Log, Metadata, Record};
use tally::{Key, Store, Value};

struct Capture(Mutex<Vec<(Level, String)>>);

impl Log for Capture {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if record.target().starts_with("tally") {
            let entry = (record.level(), record.args().to_string());
            self.0.lock().unwrap().push(entry);
        }
    }

    fn flush(&self) {}
}

static LOGGER: Capture = Capture(Mutex::new(Vec::new()));

fn append(dir: &std::path::Path, bytes: &[u8]) {
    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .open(dir.join("tally.00000000000000000000.log"))
        .unwrap();
    std::io::Write::write_all(&mut log, bytes).unwrap();
}

#[test]
fn open_warns_with_the_offset_and_where_the_bytes_went() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(LevelFilter::Trace);

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let store = Store::open(dir).unwrap();
    store
        .set(
            Key::try_from(&b"a"[..]).unwrap(),
            Value::try_from(&b"hi"[..]).unwrap(),
        )
        .unwrap();
    drop(store);

    append(dir, &[0u8; 14]);
    Store::open(dir).unwrap();
    append(dir, &[0xab; 3]);
    Store::open(dir).unwrap();
    Store::open(dir).unwrap();

    // ce01bddc32C of the three bytes, computed in Python.
    let side = dir.join("tally.log.torn-22-ce01bddc");
    assert_eq!(
        *LOGGER.0.lock().unwrap(),
        [
            (
                Level::Warn,
                "truncated a torn tail at offset 22: dropped 14 zero bytes".to_string()
            ),
            (
                Level::Warn,
                format!(
                    "truncated a torn tail at offset 22: moved 3 bytes to {}",
                    side.display()
                )
            ),
        ]
    );
    assert_eq!(std::fs::read(side).unwrap(), [0xab; 3]);
}
