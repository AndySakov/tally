//! Coverage-guided version of the model test in `src/model.rs`, on the
//! in-memory disk: on the real one, file syscalls took most of the time.

#![no_main]

use std::path::Path;

use libfuzzer_sys::fuzz_target;
use tally::fuzz::{MemFs, Model, Op, options};

fuzz_target!(|input: (bool, Vec<Op>)| {
    let (auto, ops) = input;
    let mut model = Model::new(MemFs::new(), Path::new("/db"), options(auto));
    for op in &ops {
        model.step(op);
    }
    model.finish();
});
