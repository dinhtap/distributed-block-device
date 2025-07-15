use crate::domain::*;
use crate::sectors_manager_public::*;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::vec;
use tokio::fs::File;
use tokio::fs::{read_dir, remove_file};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sha2::{Sha256, Digest};
use tokio::sync::RwLock;
use tokio::sync::RwLockWriteGuard;

const SECTOR_SIZE: usize = 4096;
const CHECKSUM_SIZE: usize = 32;
const TMPFILENAME: &str = "tmpfile";

// Use little-endian byte order
// File name is sector_idx.to_string()
// File content: 8 bytes timestamp, 1 byte write rank, 4096 bytes data
// tmpfile content: 9 bytes metadata, 4096 bytes data, 8 bytes sectorind, 32 bytes checksum

pub struct SectorsManagerImpl {
    root_path: PathBuf,
    metadata: RwLock<HashMap<SectorIdx, Arc<RwLock<(u64, u8)>>>>,
}

#[async_trait::async_trait]
impl SectorsManager for SectorsManagerImpl {
    // Filename: sectoridx_timestamp_writerank
    // tmpfile_sector_timestamp_writerank: content + checksum (32 bytes)

    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let metadata_map = self.metadata.read().await;
        match metadata_map.contains_key(&idx) {
            true => {
                let this_sector = metadata_map[&idx].clone();
                drop(metadata_map);
                let (timestamp, write_rank) = this_sector.read().await.clone();
                if (timestamp, write_rank) != (0, 0) {
                    let path = self.metadata_to_filepath(idx, timestamp, write_rank);
                    let mut file = File::open(path).await.unwrap();
                    let mut data = vec![0; SECTOR_SIZE];
                    file.read_exact(&mut data).await.unwrap();
                    SectorVec(data)
                } else {
                    SectorVec(vec![0; SECTOR_SIZE])
                }
            },
            false => SectorVec(vec![0; SECTOR_SIZE]),
        }
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let all_metadata = self.metadata.read().await;
        match all_metadata.contains_key(&idx) {
            true => {
                let this_sector = all_metadata[&idx].clone();
                drop(all_metadata);
                let ts_wr = this_sector.read().await;
                ts_wr.clone()
            },
            false => (0, 0),
        }
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let sector_data = &sector.0;
        let timestamp = sector.1;
        let write_rank = sector.2;

        let mut hasher = Sha256::new();
        hasher.update(&sector_data.0);
        let checksum = hasher.finalize();

        let mut tmpfile_content = vec![0; SECTOR_SIZE + CHECKSUM_SIZE];
        tmpfile_content[0..SECTOR_SIZE].copy_from_slice(&sector_data.0);
        tmpfile_content[SECTOR_SIZE..].copy_from_slice(&checksum);

        let rootfolder = File::open(&self.root_path).await.unwrap();
        let newfilename = idx.to_string() + "_" + &timestamp.to_string() + "_" + &write_rank.to_string();
        let tmpfilename = String::from(TMPFILENAME) + "_" + &newfilename;

        let mut all_metadata = self.metadata.write().await;
        if !all_metadata.contains_key(&idx) {
            all_metadata.insert(idx, Arc::new(RwLock::new((0, 0))));
        }
        let this_sector_metadata = all_metadata[&idx].clone();
        drop(all_metadata);
        let mut this_sector_lock = this_sector_metadata.write().await;

        // all_metadata dropped, this_sector_data remains locked as write

        let tmpfile_path = self.root_path.join(tmpfilename);
        let mut tmpfile = File::create(&tmpfile_path).await.unwrap();  
        tmpfile.write_all(&tmpfile_content).await.unwrap();
        tmpfile.sync_data().await.unwrap();
        rootfolder.sync_data().await.unwrap();

        // Handling old file if exists
        if (this_sector_lock.0, this_sector_lock.1) != (0, 0) {
            tokio::fs::remove_file(self.metadata_to_filepath(idx, this_sector_lock.0, this_sector_lock.1)).await.unwrap();
            rootfolder.sync_data().await.unwrap();
        }

        let mut newfile = File::create(self.root_path.join(newfilename)).await.unwrap();
        newfile.write_all(&sector_data.0).await.unwrap();
        newfile.sync_data().await.unwrap();
        rootfolder.sync_data().await.unwrap();

        tokio::fs::remove_file(&tmpfile_path).await.unwrap();
        rootfolder.sync_data().await.unwrap();

        this_sector_lock.0 = timestamp;
        this_sector_lock.1 = write_rank;
    }
}

impl SectorsManagerImpl {
    async fn recover(&self) {
        let rootfolder = File::open(&self.root_path).await.unwrap();

        let mut root_dir = read_dir(&self.root_path).await.unwrap();
        let mut all_files_names: Vec<String> = vec![];
        while let Some(entry) = root_dir.next_entry().await.unwrap() {
            all_files_names.push(entry.file_name().into_string().unwrap());
        }

        let mut all_metadata: RwLockWriteGuard<'_, HashMap<u64, Arc<RwLock<(u64, u8)>>>> = self.metadata.write().await;

        for filename in &all_files_names {
            if !filename.starts_with(TMPFILENAME) {
                let parts: Vec<&str> = filename.split('_').collect();
                let idx = parts[0].parse::<SectorIdx>().unwrap();
                let timestamp = parts[1].parse::<u64>().unwrap();
                let write_rank = parts[2].parse::<u8>().unwrap();
                
                all_metadata.insert(idx, Arc::new(RwLock::new((timestamp, write_rank))));
            }
        }

        for tmpfile in &all_files_names {
            if tmpfile.starts_with(TMPFILENAME) {
                self.recover_file(tmpfile, &rootfolder, &mut all_metadata).await;

                remove_file(self.root_path.join(tmpfile)).await.unwrap();
                rootfolder.sync_data().await.unwrap();
            }
        }
    }

    async fn recover_file(&self, tmpfilename: &String, rootfolder: &File, all_metadata: &mut HashMap<SectorIdx, Arc<RwLock<(u64, u8)>>>) -> Option<()> {
        let parts: Vec<&str> = tmpfilename.split('_').collect();
        let idx = parts[1].parse::<SectorIdx>().ok()?;
        let timestamp = parts[2].parse::<u64>().ok()?;
        let write_rank = parts[3].parse::<u8>().ok()?;

        let mut file = File::open(self.root_path.join(tmpfilename)).await.unwrap();
        let mut file_content = vec![0; SECTOR_SIZE + CHECKSUM_SIZE];
        
        // If tmpfile corrupted
        if file.read_exact(&mut file_content).await.is_err() {
            return Some(());
        }

        let checksum = &file_content[SECTOR_SIZE..SECTOR_SIZE + CHECKSUM_SIZE];

        let mut hasher = Sha256::new();
        hasher.update(&file_content[0..SECTOR_SIZE]);
        let checksum_calculated = hasher.finalize();

        if checksum_calculated.to_vec() != checksum {
            return Some(());
        }

        if all_metadata.contains_key(&idx) {
            let (old_ts, old_wr) = all_metadata[&idx].read().await.clone();
            let oldfilepath = self.metadata_to_filepath(idx, old_ts, old_wr);
            remove_file(oldfilepath).await.unwrap();
            rootfolder.sync_data().await.unwrap();
        }

        let mut newfile = File::create(self.root_path.join(
                                self.metadata_to_filepath(idx, timestamp, write_rank))).await.unwrap();
        newfile.write_all(&file_content[0..SECTOR_SIZE]).await.unwrap();
        rootfolder.sync_data().await.unwrap();
        
        all_metadata.insert(idx, Arc::new(RwLock::new((timestamp, write_rank))));
        Some(())
    }

    fn metadata_to_filepath(&self, idx: SectorIdx, timestamp: u64, write_rank: u8) -> PathBuf {
        self.root_path.join(idx.to_string() + "_" + &timestamp.to_string() + "_" + &write_rank.to_string())
    }

    pub async fn new_and_recover(root_path: PathBuf) -> SectorsManagerImpl {
        let sm = SectorsManagerImpl {
            root_path,
            metadata: RwLock::new(HashMap::new()),
        };
        sm.recover().await;
        sm
    }
}