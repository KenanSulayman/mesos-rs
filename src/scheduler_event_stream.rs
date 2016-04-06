use std::io::{self, Error, ErrorKind};
use std::sync::mpsc::channel;
use std::thread;

use scheduler_client::SchedulerClient;
use recordio::RecordIOCodec;
use {SchedulerConf, SchedulerRouter, util};

pub fn run_protobuf_scheduler<'a>(router: &'a mut SchedulerRouter,
                                  conf: SchedulerConf) {

    let client = SchedulerClient::new(conf.clone().master_url.to_string() +
                                      "/api/v1/scheduler",
                                      conf.clone().framework_id);
    let (tx, rx) = channel();

    let local_client = client.clone();
    let local_conf = conf.clone();
    thread::spawn(move || {
        loop {
            let mut codec = RecordIOCodec::new(tx.clone());
            let framework_info =
                util::framework_info(&*local_conf.user,
                                     &*local_conf.name,
                                     local_conf.framework_timeout
                                               .clone());
            match local_client.subscribe(framework_info) {
                Err(_) => {
                    tx.clone()
                      .send(Err(Error::new(ErrorKind::ConnectionReset,
                                           "server disconnected")))
                      .unwrap();
                }
                Ok(mut res) => {
                    match io::copy(&mut res, &mut codec) {
                        Err(e) => {
                            tx.clone().send(Err(e)).unwrap();
                        }
                        Ok(_) => (),
                    }
                }
            }
            // TODO(tyler) exponential truncated backoff
        }
    });

    router.run(rx, client, conf);
}
