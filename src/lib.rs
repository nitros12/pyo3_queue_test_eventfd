use file_descriptors::eventfd::EventFileDescriptor;
use pyo3::prelude::*;
use rand::Rng;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::AtomicBool;
use std::sync::{
    mpsc::{Receiver, Sender},
    Arc,
};
use std::time::Duration;

#[pyclass]
#[derive(Debug)]
struct BasicEvent {
    #[pyo3(get)]
    channel_id: u64,
    #[pyo3(get)]
    message_id: u64,
    #[pyo3(get)]
    worker_id: usize,
}

impl BasicEvent {
    fn new(worker_id: usize) -> Self {
        let mut rng = rand::thread_rng();
        Self {
            channel_id: rng.gen(),
            message_id: rng.gen(),
            worker_id,
        }
    }
}

#[pyclass]
struct QueueReceiver {
    pub q: Receiver<BasicEvent>,
    pub e: Arc<EventFileDescriptor>,
    pub stopped: Arc<AtomicBool>,
}

#[pymethods]
impl QueueReceiver {
    #[getter]
    fn fd(&self) -> RawFd {
        self.e.as_raw_fd()
    }

    fn get_items(&mut self, count: u8) -> Vec<BasicEvent> {
        let mut r = Vec::new();

        // perform allocation in the interpreter thread
        // ideally we'd allocate pyobjects concurrently
        for i in 0..count {
            r.push(
                self.q
                    .try_recv()
                    .unwrap_or_else(|_| panic!("Queue shouldn't have been empty here, at={}", i)),
            )
        }

        r
    }

    fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn append_task(
    task_idx: usize,
    interval: u64,
    s: Sender<BasicEvent>,
    e: Arc<EventFileDescriptor>,
    stopped: Arc<AtomicBool>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(interval));
    for _i in 0i32.. {
        if stopped.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        interval.tick().await;
        s.send(BasicEvent::new(task_idx)).unwrap();
        e.write(&1).unwrap();
    }
}

async fn close_when_done(tasks: Vec<tokio::task::JoinHandle<()>>, e: Arc<EventFileDescriptor>) {
    for task in tasks {
        let _ = task.await;
    }
    e.write(&(i64::MIN as u64)).unwrap();
    println!("closing");
}

#[pyclass]
struct TokioRT {
    pub rt: tokio::runtime::Runtime,
}

fn number_of_cpus() -> usize {
    usize::max(1, num_cpus::get())
}

#[pyfunction]
fn create_rt() -> TokioRT {
    let num_cpus = number_of_cpus();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus)
        .enable_all()
        .thread_name("testing-async-tokio-worker")
        .build()
        .unwrap();

    TokioRT { rt }
}

#[pyfunction]
fn launch_appenders(interval: u64, rt: &TokioRT) -> QueueReceiver {
    let fd = EventFileDescriptor::new(0, false).unwrap();
    let fd = Arc::new(fd);
    let (s, r) = std::sync::mpsc::channel();
    let stopped = Arc::new(AtomicBool::new(false));
    let qr = QueueReceiver {
        q: r,
        e: fd.clone(),
        stopped: stopped.clone(),
    };

    let tasks = (0..number_of_cpus())
        .map(|i| {
            rt.rt.spawn(append_task(
                i,
                interval,
                s.clone(),
                fd.clone(),
                stopped.clone(),
            ))
        })
        .collect();

    rt.rt.spawn(close_when_done(tasks, fd));

    qr
}

#[pymodule]
fn async_py_rust_queue(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(pyo3::wrap_pyfunction!(launch_appenders))?;
    m.add_wrapped(pyo3::wrap_pyfunction!(create_rt))?;
    m.add_class::<QueueReceiver>()?;

    Ok(())
}
