use chrono::{DateTime, Utc};
use std::time::UNIX_EPOCH;

pub(crate) fn is_time_zero(t: &DateTime<Utc>) -> bool {
    t.eq(&utc_zero())
}

pub(crate) fn utc_zero() -> DateTime<Utc> {
    DateTime::from(UNIX_EPOCH)
}
