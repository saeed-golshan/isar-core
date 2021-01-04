use crate::dart::{DartCObject, DartPort, DART_POST_C_OBJECT};
use isar_core::error::{illegal_state, Result};
use isar_core::instance::IsarInstance;
use isar_core::txn::IsarTxn;
use once_cell::sync::Lazy;
use std::cell::RefCell;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Mutex;
use threadpool::{Builder, ThreadPool};

thread_local! {
    static TXN: RefCell<Option<IsarTxn>> = RefCell::new(None);
}

static THREAD_POOL: Lazy<Mutex<ThreadPool>> = Lazy::new(|| Mutex::new(Builder::new().build()));

type AsyncJob = (Box<dyn FnOnce() + Send + 'static>, bool);

pub struct IsarAsyncTxn {
    tx: Sender<AsyncJob>,
    port: DartPort,
}

impl IsarAsyncTxn {
    pub fn new(isar: &'static IsarInstance, write: bool, port: DartPort) -> Self {
        eprintln!("starting txn");
        let (tx, rx): (Sender<AsyncJob>, Receiver<AsyncJob>) = mpsc::channel();
        THREAD_POOL.lock().unwrap().execute(move || {
            Self::start_executor(isar, write, rx);
        });

        IsarAsyncTxn { tx, port }
    }

    fn start_executor(isar: &IsarInstance, write: bool, rx: Receiver<AsyncJob>) {
        eprintln!("starting executor");
        let new_txn = isar.begin_txn(write);
        if let Ok(new_txn) = new_txn {
            TXN.with(|txn| {
                (*txn.borrow_mut()).replace(new_txn);
            });

            loop {
                eprintln!("loop");
                let (job, stop) = rx.recv().unwrap();
                job();
                if stop {
                    break;
                }
            }

            eprintln!("end loop");
        }
    }

    fn exec_internal<F: FnOnce() -> Result<()> + Send + 'static>(&self, job: F, stop: bool) {
        let port = self.port;
        let handle_response_job = move || {
            let result = match job() {
                Ok(()) => 0,
                Err(e) => 1,
            };
            let dart_post = DART_POST_C_OBJECT.get().unwrap();
            dart_post(port, &mut DartCObject::from_int_i32(result));
        };
        self.tx.send((Box::new(handle_response_job), stop)).unwrap();
    }

    pub fn exec<F: FnOnce(&mut IsarTxn) -> Result<()> + Send + 'static>(&self, job: F) {
        let job = || -> Result<()> {
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
        let job = || -> Result<()> {
            TXN.with(|txn| {
                let txn = txn.borrow_mut().take().unwrap();
                txn.commit()
            })?;
            Ok(())
        };
        self.exec_internal(job, true);
    }

    pub fn abort(self) {
        let job = || -> Result<()> {
            TXN.with(|txn| {
                let txn = txn.borrow_mut().take().unwrap();
                txn.abort()
            })?;
            Ok(())
        };
        self.exec_internal(job, true);
    }
}
