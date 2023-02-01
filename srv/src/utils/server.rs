//! Server module.

use log::info;
use std::io::Result;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use std::sync::atomic::{AtomicBool, Ordering};

use ctrlc;

/// Server abstract
pub struct Server {
    addr: String,
    shutdown: Arc<AtomicBool>,
}

impl Server {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn running<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(TcpStream) + Send + 'static,
    {
        let listener = TcpListener::bind(&self.addr)?;
        let local_addr = listener.local_addr()?;

        let shutdown = self.shutdown.clone();

        ctrlc::set_handler(move || {
            info!("ctrlc handle ...");

            shutdown.store(true, Ordering::Relaxed);
            let _ = TcpStream::connect(local_addr);
        })
        .expect("Error setting Ctrl-C handler");

        let server_shutdown = self.shutdown.clone();

        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                if server_shutdown.load(Ordering::Relaxed) {
                    info!("Server shutting down...");
                    return;
                }

                match stream {
                    Ok(stream) => f(stream),
                    Err(_) => break,
                }
            }
        });

        handle.join().unwrap();

        Ok(())
    }
}
