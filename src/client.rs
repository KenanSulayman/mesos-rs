use std::io;
use std::sync::{Arc, Mutex};

use hyper;
use hyper::Client;
use hyper::client::response::Response;
use hyper::header::{Accept, Connection, ContentType, Headers, Quality,
                    QualityItem, qitem};
use protobuf::{self, Message};

use proto::scheduler::{Call, Call_Accept, Call_Acknowledge, Call_Decline,
                       Call_Kill, Call_Message, Call_Reconcile,
                       Call_Reconcile_Task, Call_Request, Call_Shutdown,
                       Call_Subscribe, Call_Type};
use proto::mesos::{AgentID, ExecutorID, Filters, FrameworkID, FrameworkInfo,
                   OfferID, Operation, Request, TaskID};
use util;

#[derive(Clone)]
pub struct SchedulerClient {
    pub url: String,
    pub framework_id: Arc<Mutex<Option<FrameworkID>>>,
}

impl SchedulerClient {
    pub fn subscribe(&self,
                     framework_info: FrameworkInfo,
                     force: Option<bool>)
                     -> hyper::Result<Response> {
        let mut subscribe = Call_Subscribe::new();
        subscribe.set_framework_info(framework_info);

        let mut call = Call::new();
        call.set_field_type(Call_Type::SUBSCRIBE);
        call.set_subscribe(subscribe);
        {
            let framework_id_clone = self.framework_id.clone();
            let framework_id = framework_id_clone.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn teardown(&self) -> hyper::Result<Response> {
        let mut call = Call::new();
        call.set_field_type(Call_Type::TEARDOWN);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn accept(&self,
                  offer_ids: Vec<OfferID>,
                  operations: Vec<Operation>,
                  filters: Filters)
                  -> hyper::Result<Response> {

        let mut accept = Call_Accept::new();
        accept.set_offer_ids(protobuf::RepeatedField::from_vec(offer_ids));
        accept.set_operations(protobuf::RepeatedField::from_vec(operations));
        accept.set_filters(filters);

        let mut call = Call::new();
        call.set_field_type(Call_Type::ACCEPT);
        call.set_accept(accept);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn decline(&self,
                   offer_ids: Vec<OfferID>,
                   filters: Filters)
                   -> hyper::Result<Response> {
        let mut decline = Call_Decline::new();
        decline.set_offer_ids(protobuf::RepeatedField::from_vec(offer_ids));
        decline.set_filters(filters);

        let mut call = Call::new();
        call.set_field_type(Call_Type::DECLINE);
        call.set_decline(decline);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn revive(&self) -> hyper::Result<Response> {
        let mut call = Call::new();
        call.set_field_type(Call_Type::REVIVE);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn kill(&self,
                task_id: TaskID,
                agent_id: AgentID)
                -> hyper::Result<Response> {
        let mut kill = Call_Kill::new();
        kill.set_task_id(task_id);
        kill.set_agent_id(agent_id);

        let mut call = Call::new();
        call.set_field_type(Call_Type::KILL);
        call.set_kill(kill);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn shutdown(&self,
                    executor_id: ExecutorID,
                    agent_id: AgentID)
                    -> hyper::Result<Response> {
        let mut shutdown = Call_Shutdown::new();
        shutdown.set_executor_id(executor_id);
        shutdown.set_agent_id(agent_id);

        let mut call = Call::new();
        call.set_field_type(Call_Type::SHUTDOWN);
        call.set_shutdown(shutdown);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn acknowledge(&self,
                       agent_id: AgentID,
                       task_id: TaskID,
                       uuid: Vec<u8>)
                       -> hyper::Result<Response> {
        let mut acknowledge = Call_Acknowledge::new();
        acknowledge.set_agent_id(agent_id);
        acknowledge.set_task_id(task_id);
        acknowledge.set_uuid(uuid);

        let mut call = Call::new();
        call.set_field_type(Call_Type::ACKNOWLEDGE);
        call.set_acknowledge(acknowledge);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn reconcile_task(&self,
                          task_id: TaskID,
                          agent_id: AgentID)
                          -> hyper::Result<Response> {
        let mut reconcile = Call_Reconcile_Task::new();
        reconcile.set_task_id(task_id);
        reconcile.set_agent_id(agent_id);

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
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }


    pub fn message(&self,
                   agent_id: AgentID,
                   executor_id: ExecutorID,
                   data: Vec<u8>)
                   -> hyper::Result<Response> {
        let mut message = Call_Message::new();
        message.set_agent_id(agent_id);
        message.set_executor_id(executor_id);
        message.set_data(data);

        let mut call = Call::new();
        call.set_field_type(Call_Type::MESSAGE);
        call.set_message(message);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn request(&self, requests: Vec<Request>) -> hyper::Result<Response> {
        let mut request = Call_Request::new();
        request.set_requests(protobuf::RepeatedField::from_vec(requests));

        let mut call = Call::new();
        call.set_field_type(Call_Type::REQUEST);
        call.set_request(request);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    pub fn suppress(&self) -> hyper::Result<Response> {
        let mut call = Call::new();
        call.set_field_type(Call_Type::SUPPRESS);
        {
            let framework_id = self.framework_id.lock().unwrap();
            if framework_id.is_some() {
                let framework_id_clone = framework_id.clone();
                call.set_framework_id(framework_id_clone.unwrap());
            }
        }

        self.post(&*call.write_to_bytes().unwrap())
    }

    fn post(&self, data: &[u8]) -> hyper::Result<Response> {
        let client = Client::new();

        client.post(&*self.url)
              .headers(util::protobuf_headers())
              .body(data)
              .send()
    }
}
