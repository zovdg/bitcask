//! thread pool module.

use log::{info, warn};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

enum Message {
    NewJob(Job),
    Terminate,
}

/*
trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<Self>) {
        (*self)()
    }
}
*/

type Job = Box<dyn FnOnce() + Send + 'static>;

/// ThreadPool Definition.
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Message>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        Self {
            workers,
            sender: Some(sender),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender
            .as_ref()
            .unwrap()
            .send(Message::NewJob(job))
            .unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        info!("Sending termination message to all workers.");
        for _ in &mut self.workers {
            self.sender
                .as_mut()
                .unwrap()
                .send(Message::Terminate)
                .unwrap();
        }

        drop(self.sender.take());

        info!("Shutting down all workers...");

        for worker in &mut self.workers {
            info!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();

            match message {
                Message::NewJob(job) => {
                    info!("Worker: {id} got a job; executing.");

                    job();
                }
                Message::Terminate => {
                    warn!("Worker {id} was told to terminate.");

                    break;
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}
