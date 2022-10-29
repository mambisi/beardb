use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn is_time_zero(t: &SystemTime) -> bool {
    let t = t.duration_since(UNIX_EPOCH).unwrap().as_secs();
    let c = utc_zero().duration_since(UNIX_EPOCH).unwrap().as_secs();
    t.eq(&c)
}

pub(crate) fn utc_zero() -> SystemTime {
    SystemTime::UNIX_EPOCH
}
