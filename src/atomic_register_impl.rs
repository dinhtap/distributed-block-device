use crate::domain::*;
use crate::atomic_register_public::*;
use crate::register_client_public::*;
use crate::sectors_manager_public::*;

use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use std::marker::Send;
use uuid::Uuid;
use std::collections::HashSet;

const SECTOR_SIZE: usize = 4096;

pub struct AtomicRegisterImpl {
    self_ident: u8,
    sector_idx: SectorIdx,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    timestamp: u64,
    write_rank: u8,
    sector_data: Option<crate::SectorVec>,
    reading: bool,
    writing: bool,
    writeval: Option<crate::SectorVec>,
    readval: Option<crate::SectorVec>,
    write_phase: bool,
    acklist: HashSet<u8>,
    readlist: HashSet<u8>,
    op_id: Option<Uuid>,
    maxts: u64,
    maxrank: u8,
    client_request_identifier: u64,
    success_callback: Option<Box<
        dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync
        >>,
}

#[async_trait::async_trait]
impl AtomicRegister for AtomicRegisterImpl {
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: Box<
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                + Send
                + Sync,
        >,
    ) {
        match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.op_id = Some(Uuid::new_v4());
                self.readlist = HashSet::new();
                self.acklist = HashSet::new();
                self.reading = true;
                self.register_client.broadcast(
                    Broadcast {
                        cmd: Arc::new(SystemRegisterCommand {
                            header: SystemCommandHeader {
                                process_identifier: self.self_ident,
                                msg_ident: self.op_id.unwrap(),
                                sector_idx: self.sector_idx,
                            },
                            content: SystemRegisterCommandContent::ReadProc,
                        }),
                    }
                ).await;
            },
            ClientRegisterCommandContent::Write{data} => {
                self.op_id = Some(Uuid::new_v4());
                self.writeval = Some(data);
                self.acklist = HashSet::new();
                self.readlist = HashSet::new();
                self.writing = true;
                self.register_client.broadcast(
                    Broadcast {
                        cmd: Arc::new(SystemRegisterCommand {
                            header: SystemCommandHeader {
                                process_identifier: self.self_ident,
                                msg_ident: self.op_id.unwrap(),
                                sector_idx: self.sector_idx,
                            },
                            content: SystemRegisterCommandContent::ReadProc,
                        }),
                    }
                ).await;
            },
        }
        self.success_callback = Some(success_callback);
        self.client_request_identifier = cmd.header.request_identifier;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        match cmd.content {
            SystemRegisterCommandContent::ReadProc => {
                //TODO stop sending self
                let response = SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.self_ident,
                        msg_ident: cmd.header.msg_ident,
                        sector_idx: self.sector_idx,
                    },
                    content: SystemRegisterCommandContent::Value {
                        timestamp: self.timestamp,
                        write_rank: self.write_rank,
                        sector_data: match &self.sector_data {
                            Some(data) => data.clone(),
                            None => SectorVec(vec![0; SECTOR_SIZE]),
                        },
                    },
                };

                if cmd.header.process_identifier != self.self_ident {
                    self.register_client.send(
                        crate::Send {
                            target: cmd.header.process_identifier,
                            cmd: Arc::new(response),
                        }
                    ).await;
                }
                else {
                    self.handle_value(response.header, self.timestamp, self.write_rank, match &self.sector_data {
                        Some(data) => data.clone(),
                        None => SectorVec(vec![0; SECTOR_SIZE]),
                    }).await;
                }
                
            },
            SystemRegisterCommandContent::Value{timestamp, write_rank, sector_data} => {
                self.handle_value(cmd.header, timestamp, write_rank, sector_data.clone()).await;
            },
            SystemRegisterCommandContent::WriteProc{timestamp, write_rank, data_to_write} => {
                log::info!("WriteProc received, timestamp: {}, write_rank: {}", timestamp, write_rank);
                if (timestamp, write_rank) > (self.timestamp, self.write_rank) {
                    self.timestamp = timestamp;
                    self.write_rank = write_rank;
                    self.sector_data = Some(data_to_write.clone());
                    self.sectors_manager.write(self.sector_idx, &(data_to_write, timestamp, write_rank)).await;
                }

                let response = SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.self_ident,
                        msg_ident: cmd.header.msg_ident,
                        sector_idx: self.sector_idx,
                    },
                    content: SystemRegisterCommandContent::Ack,
                };
                if cmd.header.process_identifier != self.self_ident {
                    self.register_client.send(
                        crate::Send {
                            target: cmd.header.process_identifier,
                            cmd: Arc::new(response),
                        }
                    ).await;
                }
                else {
                    self.handle_ack(response).await;
                }
            },
            SystemRegisterCommandContent::Ack => {
                self.handle_ack(cmd).await;
            },

        }
    }
}

impl AtomicRegisterImpl {
    async fn handle_ack(&mut self, cmd: SystemRegisterCommand) {
        log::info!("Ack received");
        if self.op_id == Some(cmd.header.msg_ident) && self.write_phase {
            self.acklist.insert(cmd.header.process_identifier);
            if (self.acklist.len() > self.processes_count as usize / 2) || (self.reading || self.writing) {
                self.acklist = HashSet::new();
                self.write_phase = false;
                if self.reading {
                    self.reading = false;
                    (self.success_callback.take().unwrap())(
                        OperationSuccess {
                            request_identifier: self.client_request_identifier,
                            op_return: OperationReturn::Read(ReadReturn {
                                read_data: self.readval.clone().unwrap(),
                            }),
                        }
                    ).await;
                }
                else {
                    self.writing = false;
                    (self.success_callback.take().unwrap())(
                        OperationSuccess {
                            request_identifier: self.client_request_identifier,
                            op_return: OperationReturn::Write,
                        }
                    ).await;
                }
            }
        }
    }

    async fn handle_value(&mut self, header: SystemCommandHeader, timestamp: u64, write_rank: u8, sector_data: SectorVec) {
        if self.op_id.is_none() {
            return;
        }
        log::info!("Value received from {} sector {}, timestamp: {}, write_rank: {}", header.process_identifier, self.sector_idx, timestamp, write_rank);

        if header.msg_ident == self.op_id.unwrap() {
            self.readlist.insert(header.process_identifier);
            if self.reading || self.writing {
                if timestamp > self.maxts  || (timestamp == self.maxts && write_rank > self.maxrank){
                    self.maxts = timestamp;
                    self.maxrank = write_rank;
                    self.readval = Some(sector_data);
                }
                log::info!("Readlist size: {:?}, sector {}", self.readlist, self.sector_idx); 
                if self.readlist.len() > self.processes_count as usize / 2 {
                    if self.timestamp > self.maxts || (self.timestamp == self.maxts && self.write_rank > self.maxrank) {
                        self.maxts = timestamp;
                        self.maxrank = write_rank;
                        self.readval = match &self.sector_data {
                            Some(_) => self.sector_data.clone(),
                            None => Some(SectorVec(vec![0; SECTOR_SIZE])),
                        };
                    }
                    
                    self.readlist = HashSet::new();
                    self.acklist = HashSet::new();
                    self.write_phase = true;
                    if self.reading {
                        self.register_client.broadcast(
                            Broadcast {
                                cmd: Arc::new(SystemRegisterCommand {
                                    header: SystemCommandHeader {
                                        process_identifier: self.self_ident,
                                        msg_ident: self.op_id.unwrap(),
                                        sector_idx: self.sector_idx,
                                    },
                                    content: SystemRegisterCommandContent::WriteProc { 
                                        timestamp: self.maxts,
                                        write_rank: self.maxrank,
                                        data_to_write: self.readval.clone().unwrap(),
                                    },
                                }),
                            }
                        ).await;
                    }
                    else {
                        self.timestamp = self.maxts + 1;
                        self.write_rank = self.self_ident;
                        self.sector_data = self.writeval.clone();
                        self.sectors_manager.write(self.sector_idx, &(self.writeval.clone().unwrap(), self.timestamp, self.write_rank)).await;
                        self.register_client.broadcast(
                            Broadcast {
                                cmd: Arc::new(SystemRegisterCommand {
                                    header: SystemCommandHeader {
                                        process_identifier: self.self_ident,
                                        msg_ident: self.op_id.unwrap(),
                                        sector_idx: self.sector_idx,
                                    },
                                    content: SystemRegisterCommandContent::WriteProc { 
                                        timestamp: self.maxts + 1,
                                        write_rank: self.self_ident,
                                        data_to_write: self.writeval.clone().unwrap(),
                                    },
                                }),
                            }
                        ).await;
                    }
                }
            }
        }
    }

    pub async fn new(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Self {
        let (ts, wr) = sectors_manager.read_metadata(sector_idx).await;
        let data = match(ts, wr) {
            (0, 0) => None,
            _ => Some(sectors_manager.read_data(sector_idx).await),
        };

        Self {
            self_ident,
            sector_idx,
            register_client,
            sectors_manager,
            processes_count,
            timestamp: ts,
            write_rank: wr,
            sector_data: data,
            reading: false,
            writing: false,
            writeval: None,
            readval: None,
            write_phase: false,
            acklist: HashSet::new(),
            readlist: HashSet::new(),
            op_id: None,
            maxts: 0,
            maxrank: 0,
            client_request_identifier: 0,
            success_callback: None,
        }
    }
}