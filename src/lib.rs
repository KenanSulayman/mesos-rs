#![crate_type = "lib"]

pub mod proto;
pub mod recordio;
pub mod scheduler;
pub mod scheduler_client;
pub mod scheduler_event_stream;
pub mod scheduler_router;
pub mod util;

pub use scheduler::{Scheduler, SchedulerConf};
pub use scheduler_client::SchedulerClient;
pub use scheduler_router::{SchedulerRouter, ProtobufCallbackRouter};
pub use scheduler_event_stream::run_protobuf_scheduler;

#[macro_use] extern crate hyper;
extern crate protobuf;
extern crate itertools;
