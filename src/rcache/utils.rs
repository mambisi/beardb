use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn is_time_zero(t: &SystemTime) -> bool {
    t.eq(&utc_zero())
}

pub(crate) fn utc_zero() -> SystemTime {
    SystemTime::UNIX_EPOCH
}
