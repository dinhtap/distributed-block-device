use std::collections::HashSet;
use std::collections::HashMap;
use std::vec;

use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio::time::Duration;
use uuid::Uuid;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::register_client_public::*;
use crate::transfer_public::*;

const RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(500);
#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
pub enum MsgToRetransmit {
    ReadProc,
    WriteProc
}

pub struct RegisterClientImpl {
    tcp_locations: Arc<Vec<(String, u16)>>,
    tcp_streams: Arc<Vec<Arc<Mutex<Option<TcpStream>>>>>,
    hmac_system_key: [u8; 64],
    self_rank: u8,
    //TODO who deletes messages from this map (prob msg receiver)?, handle crashes of other processes.
    unacknowledged_messages: Arc<Mutex<HashMap<(Uuid, MsgToRetransmit), Arc<Mutex<HashSet<u8>>>>>>,
    process_count: usize,
    all_idxs: HashSet<u8>,
}

#[async_trait::async_trait]
impl RegisterClient for RegisterClientImpl {
    async fn send(&self, msg: Send) {
        let mut serialized_msg: Vec<u8> = vec![];
        serialize_register_command(&crate::RegisterCommand::System((*msg.cmd).clone()), &mut serialized_msg, &self.hmac_system_key).await.unwrap();
        let serialized = Arc::new(serialized_msg);
        send_to(self.tcp_locations.clone(), self.tcp_streams.clone(), serialized.clone(), msg.target).await;
    }

    async fn broadcast(&self, msg: Broadcast) {
        let mut serialized_msg: Vec<u8> = vec![];
        serialize_register_command(&crate::RegisterCommand::System((*msg.cmd).clone()), &mut serialized_msg, &self.hmac_system_key).await.unwrap();
        let serialized = Arc::new(serialized_msg);

        let msg_type = match msg.cmd.content {
            crate::SystemRegisterCommandContent::ReadProc => MsgToRetransmit::ReadProc,
            _ => MsgToRetransmit::WriteProc,
        };
        let unack_procs = Arc::new(Mutex::new(self.all_idxs.clone()));
        self.unacknowledged_messages.lock().await.insert((msg.cmd.header.msg_ident, msg_type), unack_procs.clone());
        let process_count = self.process_count;
        let tcp_locations = self.tcp_locations.clone();
        let tcp_streams = self.tcp_streams.clone();

        let self_rank = self.self_rank;
        tokio::spawn(async move {
            loop {
                let mut unack_lock = unack_procs.lock().await;
                log::info!("Broadcasting left from {}: {}, {}, op_id: {}", self_rank, unack_lock.len(), serialized[7], msg.cmd.header.msg_ident);
                if unack_lock.len() <= process_count / 2 {
                    log::info!("{} Broadcasting done", self_rank);
                    break;
                }

                let mut senders = vec![];
                for idx in unack_lock.iter() {
                    senders.push(tokio::spawn(send_to(tcp_locations.clone(), tcp_streams.clone(), serialized.clone(), idx.clone())));
                }
                unack_lock.remove(&self_rank);
                drop(unack_lock);
                for sender in senders {
                    sender.await.unwrap();
                }

                sleep(RETRANSMIT_TIMEOUT).await;
            }
        });
    }
}

impl RegisterClientImpl {
    pub async fn new(
        tcp_locations: &Vec<(String, u16)>,
        hmac_system_key: [u8; 64],
        self_rank: u8,
        unacknowledged_messages: Arc<Mutex<HashMap<(Uuid, MsgToRetransmit), Arc<Mutex<HashSet<u8>>>>>>,
    ) -> Self {
        let mut tcp_streams = vec![];
        for _ in 0..tcp_locations.len() {
            tcp_streams.push(Arc::new(Mutex::new(None)));
        }
        RegisterClientImpl {
            tcp_locations: Arc::new(tcp_locations.clone()),
            tcp_streams: Arc::new(tcp_streams),
            hmac_system_key,
            unacknowledged_messages,
            self_rank,
            process_count: tcp_locations.len(),
            all_idxs: (1..=tcp_locations.len() as u8).collect(),
        }
    }
}

async fn send_to(tcp_locations: Arc<Vec<(String, u16)>>, tcp_streams: Arc<Vec<Arc<Mutex<Option<TcpStream>>>>>, serialized_msg: Arc<Vec<u8>>, target: u8) {
    let tar = usize::from(target) - 1;
    let mut stream = tcp_streams[tar].lock().await;

    if stream.is_none() {
        let (host, port) = &tcp_locations[tar];
        *stream = TcpStream::connect(format!("{}:{}", host, port)).await.ok();
    }
    if stream.is_some() {
        match stream.as_mut().unwrap().write_all(&serialized_msg).await {
            Ok(_) => {}
            Err(_) => {
                *stream = None;
            }
        }
    }
}

