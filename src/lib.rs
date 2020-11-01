use file_descriptors::eventfd::EventFileDescriptor;
use pyo3::prelude::*;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::{
    mpsc::{Receiver, Sender},
    Arc,
};
use std::time::Duration;

#[pyclass]
struct QueueReceiver {
    pub q: Receiver<i32>,
    pub e: Arc<EventFileDescriptor>,
}

#[pymethods]
impl QueueReceiver {
    #[getter]
    fn fd(&self) -> RawFd {
        self.e.as_raw_fd()
    }

    fn get_items(&mut self, count: u8) -> Vec<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let mut r = Vec::new();

        // perform allocation in the interpreter thread
        // ideally we'd allocate pyobjects concurrently
        for i in 0..count {
            r.push(
                self.q
                    .try_recv()
                    .unwrap_or_else(|_| panic!("Queue shouldn't have been empty here, at={}", i))
                    .into_py(py),
            )
        }

        r
    }
}

async fn append_task(s: Sender<i32>, e: Arc<EventFileDescriptor>) {
    for i in 0i32..1000 {
        s.send(i).unwrap();
        e.write(&1).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
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

#[pyfunction]
fn create_rt() -> TokioRT {
    let rt = tokio::runtime::Runtime::new().unwrap();
    TokioRT { rt }
}

#[pyfunction]
fn launch_appenders(rt: &TokioRT) -> QueueReceiver {
    let fd = EventFileDescriptor::new(0, false).unwrap();
    let fd = Arc::new(fd);
    let (s, r) = std::sync::mpsc::channel();
    let qr = QueueReceiver {
        q: r,
        e: fd.clone(),
    };

    let tasks = (0..8)
        .map(|_| rt.rt.spawn(append_task(s.clone(), fd.clone())))
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
