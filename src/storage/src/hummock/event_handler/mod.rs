// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::HummockEpoch;
use thiserror_ext::AsReport;
use tokio::sync::{mpsc, oneshot};

use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::HummockResult;
use crate::mem_table::ImmutableMemtable;
use crate::store::{SealCurrentEpochOptions, SyncResult};

pub mod hummock_event_handler;
pub mod refiller;
pub mod uploader;

pub use hummock_event_handler::HummockEventHandler;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};

use super::store::version::HummockReadVersion;

#[derive(Debug)]
pub struct BufferWriteRequest {
    pub batch: SharedBufferBatch,
    pub epoch: HummockEpoch,
    pub grant_sender: oneshot::Sender<()>,
}

#[derive(Debug)]
pub enum HummockVersionUpdate {
    VersionDeltas(Vec<HummockVersionDelta>),
    PinnedVersion(HummockVersion),
}

pub enum HummockEvent {
    /// Notify that we may flush the shared buffer.
    BufferMayFlush,

    /// An epoch is going to be synced. Once the event is processed, there will be no more flush
    /// task on this epoch. Previous concurrent flush task join handle will be returned by the join
    /// handle sender.
    AwaitSyncEpoch {
        new_sync_epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncResult>>,
    },

    /// Clear shared buffer and reset all states
    Clear(oneshot::Sender<()>, u64),

    Shutdown,

    ImmToUploader(ImmutableMemtable),

    SealEpoch {
        epoch: HummockEpoch,
        is_checkpoint: bool,
    },

    LocalSealEpoch {
        instance_id: LocalInstanceId,
        table_id: TableId,
        epoch: HummockEpoch,
        opts: SealCurrentEpochOptions,
    },

    #[cfg(any(test, feature = "test"))]
    /// Flush all previous event. When all previous events has been consumed, the event handler
    /// will notify
    FlushEvent(oneshot::Sender<()>),

    RegisterReadVersion {
        table_id: TableId,
        new_read_version_sender: oneshot::Sender<(HummockReadVersionRef, LocalInstanceGuard)>,
        is_replicated: bool,
        vnodes: Arc<Bitmap>,
    },

    DestroyReadVersion {
        table_id: TableId,
        instance_id: LocalInstanceId,
    },
}

impl HummockEvent {
    fn to_debug_string(&self) -> String {
        match self {
            HummockEvent::BufferMayFlush => "BufferMayFlush".to_string(),

            HummockEvent::AwaitSyncEpoch {
                new_sync_epoch,
                sync_result_sender: _,
            } => format!("AwaitSyncEpoch epoch {} ", new_sync_epoch),

            HummockEvent::Clear(_, prev_epoch) => format!("Clear {:?}", prev_epoch),

            HummockEvent::Shutdown => "Shutdown".to_string(),

            HummockEvent::ImmToUploader(imm) => {
                format!("ImmToUploader {:?}", imm)
            }

            HummockEvent::SealEpoch {
                epoch,
                is_checkpoint,
            } => format!(
                "SealEpoch epoch {:?} is_checkpoint {:?}",
                epoch, is_checkpoint
            ),

            HummockEvent::LocalSealEpoch {
                epoch,
                instance_id,
                table_id,
                opts,
            } => {
                format!(
                    "LocalSealEpoch epoch: {}, table_id: {}, instance_id: {}, opts: {:?}",
                    epoch, table_id.table_id, instance_id, opts
                )
            }

            HummockEvent::RegisterReadVersion {
                table_id,
                new_read_version_sender: _,
                is_replicated,
                vnodes: _,
            } => format!(
                "RegisterReadVersion table_id {:?}, is_replicated: {:?}",
                table_id, is_replicated
            ),

            HummockEvent::DestroyReadVersion {
                table_id,
                instance_id,
            } => format!(
                "DestroyReadVersion table_id {:?} instance_id {:?}",
                table_id, instance_id
            ),

            #[cfg(any(test, feature = "test"))]
            HummockEvent::FlushEvent(_) => "FlushEvent".to_string(),
        }
    }
}

impl std::fmt::Debug for HummockEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HummockEvent")
            .field("debug_string", &self.to_debug_string())
            .finish()
    }
}

pub type LocalInstanceId = u64;
pub type HummockReadVersionRef = Arc<RwLock<HummockReadVersion>>;
pub type ReadVersionMappingType = HashMap<TableId, HashMap<LocalInstanceId, HummockReadVersionRef>>;
pub type ReadOnlyReadVersionMapping = ReadOnlyRwLockRef<ReadVersionMappingType>;

pub struct ReadOnlyRwLockRef<T>(Arc<RwLock<T>>);

impl<T> Clone for ReadOnlyRwLockRef<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> ReadOnlyRwLockRef<T> {
    pub fn new(inner: Arc<RwLock<T>>) -> Self {
        Self(inner)
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.read()
    }
}

pub struct LocalInstanceGuard {
    pub table_id: TableId,
    pub instance_id: LocalInstanceId,
    event_sender: mpsc::UnboundedSender<HummockEvent>,
}

impl Drop for LocalInstanceGuard {
    fn drop(&mut self) {
        // If sending fails, it means that event_handler and event_channel have been destroyed, no
        // need to handle failure
        self.event_sender
            .send(HummockEvent::DestroyReadVersion {
                table_id: self.table_id,
                instance_id: self.instance_id,
            })
            .unwrap_or_else(|err| {
                tracing::error!(
                    error = %err.as_report(),
                    table_id = %self.table_id,
                    instance_id = self.instance_id,
                    "LocalInstanceGuard Drop SendError",
                )
            })
    }
}
