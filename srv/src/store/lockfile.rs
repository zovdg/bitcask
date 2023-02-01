//! Lockfile implementation.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

/// A simple lockfile for `DistStorage`.
#[derive(Debug)]
pub struct Lockfile {
    handle: Option<File>,
    path: PathBuf,
}

impl Lockfile {
    /// Creates a lock at the provided `path`. Fails if lock is already exists.
    pub fn lock(path: impl AsRef<Path>) -> Result<Self, io::Error> {
        let path = path.as_ref();

        let dir_path = path.parent().expect("lock file must have a parent");
        fs::create_dir_all(dir_path)?;

        let mut lockfile_opts = fs::OpenOptions::new();
        lockfile_opts.read(true).write(true).create_new(true);

        let lockfile = lockfile_opts.open(path)?;

        Ok(Self {
            handle: Some(lockfile),
            path: path.to_path_buf(),
        })
    }
}

impl Drop for Lockfile {
    fn drop(&mut self) {
        self.handle.take();
        fs::remove_file(&self.path).expect("lock already dropped.");
    }
}
