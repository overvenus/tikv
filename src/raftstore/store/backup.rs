use std::sync::atomic::{AtomicUsize, Ordering};

pub static Number: AtomicUsize = AtomicUsize::new(0);
