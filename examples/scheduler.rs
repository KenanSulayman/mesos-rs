extern crate mesos;
extern crate crossbeam;

extern crate time;

use time::PreciseTime;

use crossbeam::scope;
use crossbeam::sync::MsQueue;

use std::sync::mpsc::channel;

use self::mesos::{
    ProtobufCallbackRouter,
    run_protobuf_scheduler,
    Scheduler,
    SchedulerClient,
    SchedulerConf
};

use self::mesos::util;
use self::mesos::proto::TaskState;

enum EventBusState {
    ONLINE,
    TASK_RUNNING(self::mesos::proto::TaskStatus),
    TASK_FINISHED(self::mesos::proto::TaskStatus)
}

static MAX_NUM_TASKS: u32 = 2;

struct TestScheduler<'a> {
    eventbus: &'a MsQueue<EventBusState>,

    max_id: u64,

    num_tasks: u32,
    running_tasks: u32
}

impl<'a> TestScheduler<'a> {
    #[inline]
    fn new (eventbus: &'a MsQueue<EventBusState>) -> TestScheduler {
        TestScheduler {
            eventbus: eventbus,

            max_id: 0,

            num_tasks: 0,
            running_tasks: 0
        }
    }

    #[inline]
    fn get_id(&mut self) -> u64 {
        self.max_id += 1;
        self.max_id
    }
}

impl<'a> Scheduler for TestScheduler<'a> {
    #[inline]
    fn subscribed(&mut self,
                  client: &SchedulerClient,
                  framework_id: &self::mesos::proto::FrameworkID,
                  heartbeat_interval_seconds: Option<f64>) {
        println!("received subscribed");

        client.reconcile(vec![]);
    }

    // Inverse offers are only available with the HTTP API
    // and are great for doing things like triggering

    // replication with stateful services before the slave
    // goes down for maintenance.

    #[inline]
    fn inverse_offers(&mut self,
                      client: &SchedulerClient,
                      inverse_offers: Vec<&self::mesos::proto::InverseOffer>) {
        println!("received inverse offers");

        // this never lets go willingly
        let offer_ids = inverse_offers.iter()
                                      .map(|o| o.get_id().clone())
                                      .collect();
        client.decline(offer_ids, None);
    }

    #[inline]
    fn offers(&mut self, client: &SchedulerClient, offers: Vec<&self::mesos::proto::Offer>) {

        // Offers are guaranteed to be for the same slave, and
        // there will be at least one.
        let slave_id = offers[0].get_slave_id();

        println!("received {} offers from slave {}",
                 offers.len(),
                 slave_id.get_value());

        let offer_ids: Vec<self::mesos::proto::OfferID> = offers.iter()
                                            .map(|o| o.get_id().clone())
                                            .collect();

        // get resources with whatever filters you need
        let mut offer_cpus: f64 = offers.iter()
                                        .flat_map(|o| o.get_resources())
                                        .filter(|r| r.get_name() == "cpus")
                                        .map(|c| c.get_scalar())
                                        .fold(0f64, |acc, cpu_res| {
                                            acc + cpu_res.get_value()
                                        });

        // or use this if you don't require special filtering
        let mut offer_mem = util::get_scalar_resource_sum("mem", offers);

        let mut tasks = vec![];
        while self.num_tasks < MAX_NUM_TASKS {
            let name = &*format!("sleepy-{}", self.get_id());

            let task_id = util::task_id(name);

            let mut command = self::mesos::proto::CommandInfo::new();
            command.set_value("env > /tmp/yolo; sleep 10".to_string());

            let mem = util::scalar("mem", "*", 128f64);
            let cpus = util::scalar("cpus", "*", 1f64);

            let resources = vec![mem, cpus];

            let task_info = util::task_info(name,
                                            &task_id,
                                            slave_id,
                                            &command,
                                            resources);
            tasks.push(task_info);

            self.num_tasks += 1;
        }

        client.launch(offer_ids, tasks, None);
    }

    #[inline]
    fn rescind(&mut self, client: &SchedulerClient, offer_id: &self::mesos::proto::OfferID) {
        println!("received rescind");
    }

    #[inline]
    fn update(&mut self, client: &SchedulerClient, status: &self::mesos::proto::TaskStatus) {
        println!("received update {:?} from {}",
                 status.get_state(),
                 status.get_task_id().get_value());

        let state = status.get_state();

        match state {
            TaskState::TASK_FINISHED => {
                self.num_tasks -= 1;
                self.running_tasks -= 1;
            },
            TaskState::TASK_RUNNING => {
                self.running_tasks += 1;
            },
            _ => {
                println!("Not implemented: {:?}", state);
            }
        }
    }

    #[inline]
    fn message(&mut self,
               client: &SchedulerClient,
               slave_id: &self::mesos::proto::SlaveID,
               executor_id: &self::mesos::proto::ExecutorID,
               data: Vec<u8>) {
        println!("received message");
    }

    #[inline]
    fn failure(&mut self,
               client: &SchedulerClient,
               slave_id: Option<&self::mesos::proto::SlaveID>,
               executor_id: Option<&self::mesos::proto::ExecutorID>,
               status: Option<i32>) {
        println!("received failure");
    }

    #[inline]
    fn error(&mut self, client: &SchedulerClient, message: String) {
        println!("received error");
    }

    #[inline]
    fn heartbeat(&mut self, client: &SchedulerClient) {
        println!("received heartbeat");
    }

    #[inline]
    fn disconnected(&mut self) {
        println!("disconnected from scheduler");
    }
}

fn main() {
    let eventbus: MsQueue<EventBusState> = MsQueue::new();
    let idbus: MsQueue<u32> = MsQueue::new();

    crossbeam::scope(|scope| {
        for i in 0..2 {
            scope.spawn(|| {
                let me = idbus.pop();

                println!("Eventbus handler {} online.", me);

                let start: u64 = time::precise_time_ns();

                loop {
                    match eventbus.pop() {
                        EventBusState::ONLINE => {
                            println!("{}: ONLINE", me);

                            let now: u64 = time::precise_time_ns() - start;
                            println!("{:?}", now);
                        },
                        EventBusState::TASK_RUNNING(status) => {
                            println!("{:?}", status);
                        },
                        EventBusState::TASK_FINISHED(status) => {
                            println!("{:?}", status);
                        }
                    }
                }
            });

            idbus.push(i);
        }

        scope.spawn(|| {
            let mut scheduler = TestScheduler::new(&eventbus);

            let conf = SchedulerConf {
                master_url: "http://localhost:5050".to_string(),
                user: "root".to_string(),
                name: "rust http".to_string(),
                framework_timeout: 0f64,
                implicit_acknowledgements: true,
                framework_id: None,
            };

            let mut router = ProtobufCallbackRouter {
                scheduler: &mut scheduler,
                conf: conf.clone()
            };

            eventbus.push(EventBusState::ONLINE);

            run_protobuf_scheduler(&mut router, conf);
        });
    });
}
