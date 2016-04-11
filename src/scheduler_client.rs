use std::sync::{Arc, Mutex};

use hyper;
use hyper::Client;
use hyper::client::response::Response;
use protobuf::{self, Message};

use proto::scheduler::{Call, Call_Accept, Call_Acknowledge, Call_Decline,
                       Call_Kill, Call_Message, Call_Reconcile,
                       Call_Reconcile_Task, Call_Request, Call_Shutdown,
                       Call_Subscribe, Call_Type};
use proto::mesos::{ExecutorID, Filters, FrameworkID, FrameworkInfo, OfferID,
                   Offer_Operation, Request, SlaveID, TaskID, TaskInfo};
use util;

#[derive(Clone)]
pub struct SchedulerClient {
    pub url: String,
    pub framework_id: Arc<Mutex<Option<FrameworkID>>>,
    pub stream_id: String,
}

impl SchedulerClient {
    pub fn new(url: String,
               framework_id: Option<FrameworkID>)
               -> SchedulerClient {
        SchedulerClient {
            url: url + "/api/v1/scheduler",
            framework_id: Arc::new(Mutex::new(framework_id)),
            stream_id: "".to_string(),
        }
    }

    pub fn get_framework_id(&self) -> Option<FrameworkID> {
        let id = self.framework_id.lock().unwrap().clone();
        id
    }

    pub fn subscribe(&mut self,
                     mut framework_info: FrameworkInfo)
                     -> hyper::Result<Response> {
        match self.get_framework_id() {
            Some(fwid) => framework_info.set_id(fwid),
            _ => (),
        }

        let mut subscribe = Call_Subscribe::new();
        subscribe.set_framework_info(framework_info);

        let mut call = Call::new();
        call.set_field_type(Call_Type::SUBSCRIBE);
        call.set_subscribe(subscribe);

        let response = self.post(&mut call);
        match response {
            Ok(ref result) => {
                match result.headers.get_raw("Mesos-Stream-Id") {
                    Some(stream_id) => {
                        let id = stream_id[0].clone();
                        self.stream_id = String::from_utf8(id).unwrap()
                    }
                    None => {}
                }
            }
            Err(_) => {}
        }

        response
    }

    pub fn teardown(&self) -> hyper::Result<Response> {
        let mut call = Call::new();
        call.set_field_type(Call_Type::TEARDOWN);

        self.post(&mut call)
    }

    pub fn launch(&self,
                  offer_ids: Vec<OfferID>,
                  tasks: Vec<TaskInfo>,
                  filters: Option<Filters>)
                  -> hyper::Result<Response> {
        let operation = util::launch_operation(tasks);
        self.accept(offer_ids, vec![operation], filters)
    }

    pub fn accept(&self,
                  offer_ids: Vec<OfferID>,
                  operations: Vec<Offer_Operation>,
                  filters: Option<Filters>)
                  -> hyper::Result<Response> {

        let mut accept = Call_Accept::new();
        accept.set_offer_ids(protobuf::RepeatedField::from_vec(offer_ids));
        accept.set_operations(protobuf::RepeatedField::from_vec(operations));
        if filters.is_some() {
            accept.set_filters(filters.unwrap());
        }

        let mut call = Call::new();
        call.set_field_type(Call_Type::ACCEPT);
        call.set_accept(accept);

        self.post(&mut call)
    }

    pub fn decline(&self,
                   offer_ids: Vec<OfferID>,
                   filters: Option<Filters>)
                   -> hyper::Result<Response> {
        let mut decline = Call_Decline::new();
        decline.set_offer_ids(protobuf::RepeatedField::from_vec(offer_ids));
        if filters.is_some() {
            decline.set_filters(filters.unwrap());
        }

        let mut call = Call::new();
        call.set_field_type(Call_Type::DECLINE);
        call.set_decline(decline);

        self.post(&mut call)
    }

    pub fn revive(&self) -> hyper::Result<Response> {
        let mut call = Call::new();
        call.set_field_type(Call_Type::REVIVE);

        self.post(&mut call)
    }

    pub fn kill(&self,
                task_id: TaskID,
                slave_id: Option<SlaveID>)
                -> hyper::Result<Response> {
        let mut kill = Call_Kill::new();
        kill.set_task_id(task_id);
        if slave_id.is_some() {
            kill.set_slave_id(slave_id.unwrap());
        }

        let mut call = Call::new();
        call.set_field_type(Call_Type::KILL);
        call.set_kill(kill);

        self.post(&mut call)
    }

    pub fn shutdown(&self,
                    executor_id: ExecutorID,
                    slave_id: SlaveID)
                    -> hyper::Result<Response> {
        let mut shutdown = Call_Shutdown::new();
        shutdown.set_executor_id(executor_id);
        shutdown.set_slave_id(slave_id);

        let mut call = Call::new();
        call.set_field_type(Call_Type::SHUTDOWN);
        call.set_shutdown(shutdown);

        self.post(&mut call)
    }

    pub fn acknowledge(&self,
                       slave_id: SlaveID,
                       task_id: TaskID,
                       uuid: Vec<u8>)
                       -> hyper::Result<Response> {
        let mut acknowledge = Call_Acknowledge::new();
        acknowledge.set_slave_id(slave_id);
        acknowledge.set_task_id(task_id);
        acknowledge.set_uuid(uuid);

        let mut call = Call::new();
        call.set_field_type(Call_Type::ACKNOWLEDGE);
        call.set_acknowledge(acknowledge);

        self.post(&mut call)
    }

    pub fn reconcile_task(&self,
                          task_id: TaskID,
                          slave_id: Option<SlaveID>)
                          -> hyper::Result<Response> {
        let mut reconcile = Call_Reconcile_Task::new();
        reconcile.set_task_id(task_id);
        if slave_id.is_some() {
            reconcile.set_slave_id(slave_id.unwrap());
        }

        self.reconcile(vec![reconcile])
    }

    pub fn reconcile(&self,
                     tasks: Vec<Call_Reconcile_Task>)
                     -> hyper::Result<Response> {
        let mut reconcile = Call_Reconcile::new();
        reconcile.set_tasks(protobuf::RepeatedField::from_vec(tasks));

        let mut call = Call::new();
        call.set_field_type(Call_Type::RECONCILE);
        call.set_reconcile(reconcile);

        self.post(&mut call)
    }


    pub fn message(&self,
                   slave_id: SlaveID,
                   executor_id: ExecutorID,
                   data: Vec<u8>)
                   -> hyper::Result<Response> {
        let mut message = Call_Message::new();
        message.set_slave_id(slave_id);
        message.set_executor_id(executor_id);
        message.set_data(data);

        let mut call = Call::new();
        call.set_field_type(Call_Type::MESSAGE);
        call.set_message(message);

        self.post(&mut call)
    }

    pub fn request(&self, requests: Vec<Request>) -> hyper::Result<Response> {
        let mut request = Call_Request::new();
        request.set_requests(protobuf::RepeatedField::from_vec(requests));

        let mut call = Call::new();
        call.set_field_type(Call_Type::REQUEST);
        call.set_request(request);

        self.post(&mut call)
    }

    pub fn suppress(&self) -> hyper::Result<Response> {
        let mut call = Call::new();
        call.set_field_type(Call_Type::SUPPRESS);

        self.post(&mut call)
    }

    fn post(&self, call: &mut Call) -> hyper::Result<Response> {
        match self.get_framework_id() {
            Some(fwid) => call.set_framework_id(fwid),
            _ => (),
        }

        let client = Client::new();

        let data = &*call.write_to_bytes().unwrap();

        client.post(&*self.url)
              .headers(util::protobuf_headers(self.stream_id.clone()))
              .body(data)
              .send()
    }
}
