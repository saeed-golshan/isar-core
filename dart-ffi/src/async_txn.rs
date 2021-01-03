use crate::raw_object_set::RawObjectSet;
use isar_core::error::{illegal_state, IsarError, Result};
use isar_core::instance::IsarInstance;
use isar_core::txn::IsarTxn;
use std::cell::RefCell;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use threadpool::{Builder, ThreadPool};

thread_local! {
    static THREAD_POOL: ThreadPool = Builder::new().build();
    static TXN: RefCell<Option<IsarTxn>> = RefCell::new(None);
}

type AsyncJob = (Box<dyn FnOnce() + Send + 'static>, bool);

pub struct IsarAsyncTxn {
    tx: Sender<AsyncJob>,
    handle: i64,
}

impl IsarAsyncTxn {
    pub fn new(isar: &'static IsarInstance, write: bool, handle: i64) -> Self {
        let (tx, rx): (Sender<AsyncJob>, Receiver<AsyncJob>) = mpsc::channel();
        THREAD_POOL.with(|tp| {
            tp.execute(move || {
                Self::start_executor(isar, write, rx);
            });
        });

        IsarAsyncTxn { tx, handle }
    }

    fn start_executor(isar: &IsarInstance, write: bool, rx: Receiver<AsyncJob>) {
        let new_txn = isar.begin_txn(write);
        if let Ok(new_txn) = new_txn {
            TXN.with(|txn| {
                (*txn.borrow_mut()).replace(new_txn);
            });

            loop {
                let (job, stop) = rx.recv().unwrap();
                job();
                if stop {
                    break;
                }
            }
        }
    }

    fn exec_internal<F: FnOnce() -> Result<AsyncResponse> + Send + 'static>(
        &self,
        job: F,
        stop: bool,
    ) {
        let handle_response_job = || {
            let response = match job() {
                Ok(result) => result,
                Err(e) => AsyncResponse::error(e),
            };
        };
        self.tx.send((Box::new(handle_response_job), stop)).unwrap();
    }

    pub fn exec<F: FnOnce(&mut IsarTxn) -> Result<AsyncResponse> + Send + 'static>(&self, job: F) {
        let job = || -> Result<AsyncResponse> {
            TXN.with(|txn| {
                if let Some(ref mut txn) = *txn.borrow_mut() {
                    job(txn)
                } else {
                    illegal_state("Transaction not available.")
                }
            })
        };
        self.exec_internal(job, false);
    }

    pub fn commit(self) {
        let job = || -> Result<AsyncResponse> {
            TXN.with(|txn| {
                let txn = txn.borrow_mut().take().unwrap();
                txn.commit()
            })?;
            Ok(AsyncResponse::success())
        };
        self.exec_internal(job, true);
    }

    pub fn abort(self) {
        let job = || -> Result<AsyncResponse> {
            TXN.with(|txn| {
                let txn = txn.borrow_mut().take().unwrap();
                txn.abort()
            })?;
            Ok(AsyncResponse::success())
        };
        self.exec_internal(job, true);
    }
}

#[repr(C)]
pub struct AsyncResponse {
    pub data: Option<RawObjectSet>,
    pub count: u32,
    pub error: u32,
}

impl AsyncResponse {
    pub fn success() -> Self {
        AsyncResponse {
            count: 0,
            data: None,
            error: 0,
        }
    }

    pub fn error(err: IsarError) -> Self {
        AsyncResponse {
            count: 0,
            data: None,
            error: 1,
        }
    }

    pub fn data(objects: RawObjectSet) -> Self {
        AsyncResponse {
            count: objects.length(),
            data: Some(objects),
            error: 1,
        }
    }
}
