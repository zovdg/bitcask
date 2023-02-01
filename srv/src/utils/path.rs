//! path utils

use std::path::Path;

#[allow(dead_code)]
pub fn parse_file_id(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .split('.')
        .next()?
        .parse::<u64>()
        .ok()
}
