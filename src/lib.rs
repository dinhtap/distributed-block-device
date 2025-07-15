mod domain;
mod atomic_register_impl;
mod sectors_manager_impl;
mod register_client_impl;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;

use tokio::{io::AsyncWriteExt, sync::mpsc::UnboundedSender, sync::mpsc::channel};

use hmac::{Hmac, Mac};
use sha2::Sha256;

const SECTOR_SIZE: usize = 4096;

const READRETURN_CODE: u8 = 0x41;
const WRITERETURN_CODE: u8 = 0x42;
async fn serialize_client_response(status_code: StatusCode, msg_type: u8, request_number: u64, response_content: Option<SectorVec>, hmac_client_key: &[u8; 32]) -> Vec<u8> {
    let mut serialized: Vec<u8> = vec![0; 16];
    serialized[0..4].copy_from_slice(&MAGIC_NUMBER);
    serialized[6] = status_code as u8;
    serialized[7] = msg_type;
    serialized[8..16].copy_from_slice(&request_number.to_be_bytes());
    if response_content.is_some() {
        serialized.extend_from_slice(response_content.unwrap().0.as_slice());
    }

    let hmac_tag = calculate_hmac_tag(&serialized, hmac_client_key);
    serialized.extend_from_slice(&hmac_tag);
    return serialized;
}

fn calculate_hmac_tag(data: &[u8], key: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().into()
}


pub async fn run_register_process(config: Configuration) {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::sync::{Mutex, mpsc, RwLock};
    use uuid::Uuid;
    use crate::register_client_impl::*;

    let config_idx = config.public.self_rank as usize - 1;
    let listener_addr = config.public.tcp_locations[config_idx].0.clone() + ":" + &config.public.tcp_locations[config_idx].1.to_string();
    let listener = tokio::net::TcpListener::bind(listener_addr).await.unwrap();

    let unacknowledged_messages = Arc::new(Mutex::new(HashMap::<(Uuid, MsgToRetransmit), Arc<Mutex<HashSet<u8>>>>::new()));
    let register_client = Arc::new(RegisterClientImpl::new(
        &config.public.tcp_locations,
        config.hmac_system_key,
        config.public.self_rank,
        unacknowledged_messages.clone(),
    ).await);
    let sectors_manager = sectors_manager_public::build_sectors_manager(config.public.storage_dir).await;
    let atomic_register_tasks = Arc::new(Mutex::new(HashSet::<u64>::new()));
    // 2 queues, first for client, second for system
    let atomic_register_msgqueues = Arc::new(RwLock::new(HashMap::<u64, (UnboundedSender<(ClientRegisterCommand, UnboundedSender<Vec<u8>>)>, UnboundedSender<SystemRegisterCommand>)>::new()));

    let process_count = config.public.tcp_locations.len() as u8;

    // TODO listen and serve
    loop {
        let stream = match listener.accept().await {
            Ok((str, _)) => str,
            Err(_) => continue,
        };

        let unack_msg = unacknowledged_messages.clone();
        let sec_manager = sectors_manager.clone();
        let reg_client = register_client.clone();
        let atomic_reg_tasks = atomic_register_tasks.clone();
        let atomic_reg_msgqueues = atomic_register_msgqueues.clone();
        // every task works only on 1 register, init new register task and msgqueue if not yet
        tokio::spawn(async move {
            let (mut read_stream, mut write_stream) = stream.into_split();

            // Client output handler
            let (client_send_tx, mut client_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            tokio::spawn(async move {
                loop {
                    let msg = client_rx.recv().await.unwrap();
                    match write_stream.write_all(&msg).await {
                        Ok(_) => {
                            log::info!("Sent response, {}", msg[7]);
                        },
                        Err(_) => return,
                    }
                }
            });

            loop {
                let (received_msg, hmac_valid) = match deserialize_register_command(&mut read_stream, &config.hmac_system_key, &config.hmac_client_key).await{
                    Ok((msg, val)) => (msg, val),
                    Err(_) => return,
                };

                // TODO init atomic register task if not yet, append msg to queue
                let sector_idx = match &received_msg {
                    RegisterCommand::Client(clientcmd) => clientcmd.header.sector_idx,
                    RegisterCommand::System(systemcmd) => systemcmd.header.sector_idx,
                };

                match &received_msg {
                    RegisterCommand::System(systemcmd) => {
                        log::info!("Received system command, from {} to {}, sector {}", systemcmd.header.process_identifier, config.public.self_rank, sector_idx);
                    }  
                    _ => (),
                }

                if !hmac_valid {
                    match &received_msg {
                        RegisterCommand::Client(clientcmd) => {
                            // invalid hmac response
                            let msg_type = match clientcmd.content {
                                ClientRegisterCommandContent::Read => {
                                    READRETURN_CODE
                                },
                                ClientRegisterCommandContent::Write{..} => {
                                    WRITERETURN_CODE
                                },
                            };
                            let serialized = serialize_client_response(StatusCode::AuthFailure, msg_type, clientcmd.header.request_identifier, None, &config.hmac_client_key).await;
                            client_send_tx.send(serialized).unwrap();
                        },
                        RegisterCommand::System(_) => (),
                    }
                    continue;
                }
                else if sector_idx >= config.public.n_sectors {
                    match &received_msg {
                        RegisterCommand::Client(clientcmd) => {
                            // invalid sector index response
                            let msg_type = match clientcmd.content {
                                ClientRegisterCommandContent::Read => {
                                    READRETURN_CODE
                                },
                                ClientRegisterCommandContent::Write{..} => {
                                    WRITERETURN_CODE
                                },
                            };
                            let serialized = serialize_client_response(StatusCode::InvalidSectorIndex, msg_type, clientcmd.header.request_identifier, None, &config.hmac_client_key).await;
                            client_send_tx.send(serialized).unwrap();
                        },
                        RegisterCommand::System(_) => (),
                    }
                    continue;
                }
                
                // task running atomic register if not yet
                let mut register_tasks_lock = atomic_reg_tasks.lock().await;
                if !register_tasks_lock.contains(&sector_idx) {
                    let (client_recv_tx, mut client_recv_rx) = mpsc::unbounded_channel();
                    let (systemtx, mut systemrx) = mpsc::unbounded_channel();
                    atomic_reg_msgqueues.write().await.insert(sector_idx.clone(), (client_recv_tx, systemtx));
                    register_tasks_lock.insert(sector_idx.clone());
                    drop(register_tasks_lock);
                    let secmanager = sec_manager.clone();
                    let regclient = reg_client.clone();
                    
                    tokio::spawn(async move {
                        let mut atomic_register = atomic_register_public::build_atomic_register(
                            config.public.self_rank,
                            sector_idx,
                            regclient,
                            secmanager,
                            process_count,
                        ).await;
                        let serve_client = Arc::new(Mutex::new(true));
                        // to wake up to pick client msg
                        let (wake_tx, mut wake_rx) = channel(1);
                        loop {
                            let serve_client_lock = serve_client.lock().await;
                            match *serve_client_lock {
                                false => {
                                    drop(serve_client_lock);
                                    tokio::select! {
                                        systemcmd = systemrx.recv() => {
                                            atomic_register.system_command(systemcmd.unwrap()).await;
                                        },
                                        _ = wake_rx.recv() => {
                                            continue;
                                        },
                                    }
                                },
                                true => {
                                    let wake_tx_clone = wake_tx.clone();
                                    drop(serve_client_lock);
                                    tokio::select! {
                                        msg = client_recv_rx.recv() => {
                                            let mut serve_client_lock = serve_client.lock().await;
                                            *serve_client_lock = false;
                                            drop(serve_client_lock);
                                            let (clientcmd, c_tx) = msg.unwrap();
                                            log::info!("got Client command, {}, sector {}", config.public.self_rank, sector_idx);

                                            let serve_cl = serve_client.clone();
                                            
                                            let success_callback: Box<
                                                dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + core::marker::Send>>
                                                + core::marker::Send
                                                + Sync,
                                            > = Box::new(move |op_success| {
                                                Box::pin(async move {
                                                    let msg_type = match op_success.op_return {
                                                        OperationReturn::Read(_) => {
                                                            READRETURN_CODE
                                                        },
                                                        OperationReturn::Write => {
                                                            WRITERETURN_CODE
                                                        },
                                                    };
                                                    let response_content = match op_success.op_return {
                                                        OperationReturn::Read(read_data) => Some(read_data.read_data),
                                                        OperationReturn::Write => None,
                                                    };
                                                    let serialized = serialize_client_response(StatusCode::Ok, msg_type, op_success.request_identifier, response_content, &config.hmac_client_key).await;
                                                    c_tx.send(serialized).unwrap();
                                                    *serve_cl.lock().await = true;
                                                    wake_tx_clone.try_send(()).unwrap_or_default();
                                                    log::info!("Called callback, {}", msg_type);
                                                })
                                            });
                                            atomic_register.client_command(clientcmd, success_callback).await;
                                        },
                                        msg = systemrx.recv() => {
                                            let systemcmd = msg.unwrap();
                                            atomic_register.system_command(systemcmd).await;
                                        }
                                    }
                                },
                            };
                        }
                    });
                }

                // check whether msg is an acknowledgment, and remove from broadcasting list
                match &received_msg {
                    RegisterCommand::System(systemcmd) => {
                        match &systemcmd.content {
                            SystemRegisterCommandContent::Ack | SystemRegisterCommandContent::Value {..} => {
                                let mut unack_msg_lock = unack_msg.lock().await;
                                let msg_ack_type = match &systemcmd.content {
                                    SystemRegisterCommandContent::Ack => MsgToRetransmit::WriteProc,
                                    SystemRegisterCommandContent::Value {..} => MsgToRetransmit::ReadProc,
                                    _ => MsgToRetransmit::WriteProc,
                                };

                                if unack_msg_lock.contains_key(&(systemcmd.header.msg_ident, msg_ack_type)) {
                                    let set_mutex = unack_msg_lock[&(systemcmd.header.msg_ident, msg_ack_type)].clone();
                                    drop(unack_msg_lock);
                                    let mut set_lock = set_mutex.lock().await;
                                    set_lock.remove(&systemcmd.header.process_identifier);
                                    if set_lock.is_empty() {
                                        drop(set_lock);
                                        unack_msg_lock = unack_msg.lock().await;
                                        unack_msg_lock.remove(&(systemcmd.header.msg_ident, msg_ack_type));
                                        log::info!("Removed ack {}, {}", systemcmd.header.msg_ident, systemcmd.header.process_identifier);
                                    }
                                }
                            },
                            _ => (),
                        }
                        let tx = atomic_reg_msgqueues.read().await.get(&sector_idx).unwrap().1.clone();
                        tx.send(systemcmd.clone()).unwrap();       
                    },
                    RegisterCommand::Client(clientcmd) => {
                        let tx = atomic_reg_msgqueues.read().await.get(&sector_idx).unwrap().0.clone();
                        tx.send((clientcmd.clone(), client_send_tx.clone())).unwrap();
                    },
                };    
            }
        });
    }
}

pub mod atomic_register_public {
    use crate::atomic_register_impl::AtomicRegisterImpl;
    use crate::{
        ClientRegisterCommand, OperationSuccess, RegisterClient, SectorIdx, SectorsManager,
        SystemRegisterCommand
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Handle a client command. After the command is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        ///
        /// This function corresponds to the handlers of Read and Write events in the
        /// (N,N)-AtomicRegister algorithm.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Handle a system command.
        ///
        /// This function corresponds to the handlers of READ_PROC, VALUE, WRITE_PROC
        /// and ACK messages in the (N,N)-AtomicRegister algorithm.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Communication with other processes of the system is to be done by register_client.
    /// And sectors must be stored in the sectors_manager instance.
    ///
    /// This function corresponds to the handlers of Init and Recovery events in the
    /// (N,N)-AtomicRegister algorithm.
    pub async fn build_atomic_register(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Box<dyn AtomicRegister> {
        // TODO recovery, ALL
        
        Box::new(AtomicRegisterImpl::new(
            self_ident,
            sector_idx,
            register_client,
            sectors_manager,
            processes_count,
        ).await)
    }
}



pub mod sectors_manager_public {
    use crate::sectors_manager_impl::SectorsManagerImpl;
    use crate::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        /// Returns 4096 bytes of sector data by index.
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;

        /// Returns timestamp and write rank of the process which has saved this data.
        /// Timestamps and ranks are relevant for atomic register algorithm, and are described
        /// there.
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        Arc::new(SectorsManagerImpl::new_and_recover(path).await)
    }
}

pub mod transfer_public {
    use crate::{ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent,
            RegisterCommand, SystemCommandHeader, SystemRegisterCommand, 
            SystemRegisterCommandContent, MAGIC_NUMBER, SECTOR_SIZE, calculate_hmac_tag};
    use std::{io::Error, vec};
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, AsyncReadExt};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use uuid::Uuid;

    const READ_MSG_TYPE: u8 = 0x01;
    const WRITE_MSG_TYPE: u8 = 0x02;
    const READPROC_MSG_TYPE: u8 = 0x03;
    const VALUE_MSG_TYPE: u8 = 0x04;
    const WRITEPROC_MSG_TYPE: u8 = 0x05;
    const ACK_MSG_TYPE: u8 = 0x06;

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), Error> {
        let mut register_command: RegisterCommand = RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: 0,
                sector_idx: 0,
            },
            content: ClientRegisterCommandContent::Read,
        });// dummy initialization
        let mut msg_content: Vec<u8> = vec![];
        let mut msg_type: u8;
        loop {
            let mut four_bytes_buf: [u8; 4] = [0; 4];
            data.read_exact(&mut four_bytes_buf).await?;
            while four_bytes_buf != MAGIC_NUMBER {
                let mut buf: [u8; 1] = [0; 1];
                data.read_exact(&mut buf).await?;
                four_bytes_buf.rotate_left(1);
                four_bytes_buf[3] = buf[0];
            }
            msg_content.extend_from_slice(&four_bytes_buf);
            
            data.read_exact(&mut four_bytes_buf).await?;
            msg_content.extend_from_slice(&four_bytes_buf);
            msg_type = four_bytes_buf[3];

            if msg_type != READ_MSG_TYPE && msg_type != WRITE_MSG_TYPE && msg_type != READPROC_MSG_TYPE && msg_type != VALUE_MSG_TYPE && msg_type != WRITEPROC_MSG_TYPE && msg_type != ACK_MSG_TYPE {
                continue;
            }

            if msg_type == READ_MSG_TYPE || msg_type == WRITE_MSG_TYPE {
                let mut header_buf: [u8; 16] = [0; 16];
                data.read_exact(&mut header_buf).await?;
                msg_content.extend_from_slice(&header_buf);

                let client_command_header: ClientCommandHeader = ClientCommandHeader {
                    request_identifier: u64::from_be_bytes(header_buf[0..8].try_into().unwrap()),
                    sector_idx: u64::from_be_bytes(header_buf[8..16].try_into().unwrap()),
                };
                
                let client_command_content = 
                    if msg_type == READ_MSG_TYPE {
                        ClientRegisterCommandContent::Read
                    } else {
                        let mut data_buf: Vec<u8> = vec![0; SECTOR_SIZE];
                        data.read_exact(&mut data_buf).await?;
                        msg_content.extend_from_slice(&data_buf);
                        ClientRegisterCommandContent::Write{data: crate::SectorVec(data_buf)}
                    };
                
                register_command = RegisterCommand::Client(ClientRegisterCommand {
                    header: client_command_header,
                    content: client_command_content,
                });
            }

            else if msg_type == READPROC_MSG_TYPE || msg_type == VALUE_MSG_TYPE || msg_type == WRITEPROC_MSG_TYPE || msg_type == ACK_MSG_TYPE {
                let mut header_buf: [u8; 24] = [0; 24];
                data.read_exact(&mut header_buf).await?;
                msg_content.extend_from_slice(&header_buf);

                let system_command_header = SystemCommandHeader {
                    process_identifier: msg_content[6],
                    msg_ident: Uuid::from_slice(&header_buf[0..16]).unwrap(),
                    sector_idx: u64::from_be_bytes(header_buf[16..24].try_into().unwrap()),
                };

                let system_command_content = 
                    if msg_type == READPROC_MSG_TYPE {
                        SystemRegisterCommandContent::ReadProc
                    } 
                    else if msg_type == ACK_MSG_TYPE {
                        SystemRegisterCommandContent::Ack
                    }
                    else {
                        let mut content_buf: Vec<u8> = vec![0; 16];
                        data.read_exact(&mut content_buf).await?;
                        msg_content.extend_from_slice(&content_buf);
                        let timestamp = u64::from_be_bytes(content_buf[0..8].try_into().unwrap());
                        let write_rank = content_buf[15];
                        let mut data_buf: Vec<u8> = vec![0; SECTOR_SIZE];
                        data.read_exact(&mut data_buf).await?;
                        msg_content.extend_from_slice(&data_buf);
                        if msg_type == VALUE_MSG_TYPE {
                            SystemRegisterCommandContent::Value {
                                timestamp,
                                write_rank,
                                sector_data: crate::SectorVec(data_buf),
                            }
                        } else {
                            SystemRegisterCommandContent::WriteProc {
                                timestamp,
                                write_rank,
                                data_to_write: crate::SectorVec(data_buf),
                            }
                        }
                    };

                register_command = RegisterCommand::System(SystemRegisterCommand {
                    header: system_command_header,
                    content: system_command_content,
                });
            }

            let mut hmac_tag: [u8; 32] = [0; 32];
            data.read_exact(&mut hmac_tag).await?;
            let mut mac = 
                if msg_type == READ_MSG_TYPE || msg_type == WRITE_MSG_TYPE {
                    Hmac::<Sha256>::new_from_slice(hmac_client_key).unwrap()
                } else {
                    Hmac::<Sha256>::new_from_slice(hmac_system_key).unwrap()
                };
            
            mac.update(&msg_content);
            let hmac_valid = mac.verify(&hmac_tag.try_into().unwrap()).is_ok();
            return Ok((register_command, hmac_valid))
        }
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        match cmd {
            RegisterCommand::Client(clientcmd) => {
                let mut serialized: Vec<u8> = vec![0; 24];
                serialized[0..4].copy_from_slice(&MAGIC_NUMBER);
                match &clientcmd.content {
                    ClientRegisterCommandContent::Read => serialized[7] = READ_MSG_TYPE,
                    ClientRegisterCommandContent::Write{data} =>{
                        serialized[7] = WRITE_MSG_TYPE;
                        serialized.extend_from_slice(&data.0);
                    },
                };
                serialized[8..16].copy_from_slice(&clientcmd.header.request_identifier.to_be_bytes());
                serialized[16..24].copy_from_slice(&clientcmd.header.sector_idx.to_be_bytes());
                
                let hmac_tag = calculate_hmac_tag(&serialized, hmac_key);
                serialized.extend_from_slice(&hmac_tag);

                writer.write_all(&serialized).await
            },
            RegisterCommand::System(systemcmd) => {
                let mut serialized = vec![0; 32];
                serialized[0..4].copy_from_slice(&MAGIC_NUMBER);
                serialized[6] = systemcmd.header.process_identifier;
                serialized[8..24].copy_from_slice(systemcmd.header.msg_ident.as_bytes());
                serialized[24..32].copy_from_slice(&systemcmd.header.sector_idx.to_be_bytes());
                match &systemcmd.content {
                    SystemRegisterCommandContent::ReadProc => serialized[7] = READPROC_MSG_TYPE,
                    SystemRegisterCommandContent::Value{timestamp, write_rank, sector_data} => {
                        serialized[7] = VALUE_MSG_TYPE;
                        let mut content: Vec<u8> = vec![0; 16];
                        content[0..8].copy_from_slice(&timestamp.to_be_bytes());
                        content[15] = write_rank.to_be_bytes()[0];
                        serialized.extend_from_slice(&content);
                        serialized.extend_from_slice(&sector_data.0);
                    },
                    SystemRegisterCommandContent::WriteProc{timestamp, write_rank, data_to_write} => {
                        serialized[7] = WRITEPROC_MSG_TYPE;
                        let mut content: Vec<u8> = vec![0; 16];
                        content[0..8].copy_from_slice(&timestamp.to_be_bytes());
                        content[15] = write_rank.to_be_bytes()[0];
                        serialized.extend_from_slice(&content);
                        serialized.extend_from_slice(&data_to_write.0);
                    },
                    SystemRegisterCommandContent::Ack => serialized[7] = ACK_MSG_TYPE,
                };
                
                let hmac_tag = calculate_hmac_tag(&serialized, hmac_key);
                serialized.extend_from_slice(&hmac_tag);
                writer.write_all(&serialized).await
            },
        }
    }
}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in AtomicRegister. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + core::marker::Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: u8,
    }

}
