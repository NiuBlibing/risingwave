// Copyright 2023 RisingWave Labs
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

use std::alloc::{Allocator, Global};
use std::cmp::min;
use std::hash::{BuildHasher, Hash};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lru::DefaultHasher;
use risingwave_common::estimate_size::collections::lru::EstimatedLruCache;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::util::epoch::Epoch;

use crate::common::metrics::MetricsInfo;

/// The managed cache is a lru cache that bounds the memory usage by epoch.
/// Should be used with `GlobalMemoryManager`.
pub struct ManagedLruCache<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    inner: EstimatedLruCache<K, V, S, A>,
    /// The entry with epoch less than water should be evicted.
    /// Should only be updated by the `GlobalMemoryManager`.
    watermark_epoch: Arc<AtomicU64>,
    metrics_info: Option<MetricsInfo>,
}

impl<K: Hash + Eq + EstimateSize, V: EstimateSize, S: BuildHasher, A: Clone + Allocator>
    ManagedLruCache<K, V, S, A>
{
    /// Evict epochs lower than the watermark
    pub fn evict(&mut self) {
        let epoch = self.watermark_epoch.load(Ordering::Relaxed);
        self.inner.evict_by_epoch(epoch);
        self.report_evicted_watermark(epoch);
    }

    /// Evict epochs lower than the watermark, except those entry which touched in this epoch
    pub fn evict_except_cur_epoch(&mut self) {
        let epoch = self.watermark_epoch.load(Ordering::Relaxed);
        let epoch = min(epoch, self.inner.current_epoch());
        self.inner.evict_by_epoch(epoch);
        self.report_evicted_watermark(epoch);
    }

    fn report_evicted_watermark(&self, epoch: u64) {
        if let Some(metrics_info) = self.metrics_info.as_ref() {
            metrics_info
                .metrics
                .lru_evicted_watermark_time_ms
                .with_label_values(&[&metrics_info.table_id, &metrics_info.actor_id])
                .set(Epoch(epoch).physical_time() as _);
        };
    }
}

impl<K, V, S, A: Clone + Allocator> Deref for ManagedLruCache<K, V, S, A> {
    type Target = EstimatedLruCache<K, V, S, A>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, S, A: Clone + Allocator> DerefMut for ManagedLruCache<K, V, S, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub fn new_unbounded<K: Hash + Eq + EstimateSize, V: EstimateSize>(
    watermark_epoch: Arc<AtomicU64>,
) -> ManagedLruCache<K, V> {
    ManagedLruCache {
        inner: EstimatedLruCache::unbounded(),
        watermark_epoch,
        metrics_info: None,
    }
}

pub fn new_unbounded_with_metrics<K: Hash + Eq + EstimateSize, V: EstimateSize>(
    watermark_epoch: Arc<AtomicU64>,
    metrics_info: MetricsInfo,
) -> ManagedLruCache<K, V> {
    ManagedLruCache {
        inner: EstimatedLruCache::unbounded(),
        watermark_epoch,
        metrics_info: Some(metrics_info),
    }
}

pub fn new_with_hasher_in<
    K: Hash + Eq + EstimateSize,
    V: EstimateSize,
    S: BuildHasher,
    A: Clone + Allocator,
>(
    watermark_epoch: Arc<AtomicU64>,
    metrics_info: MetricsInfo,
    hasher: S,
    alloc: A,
) -> ManagedLruCache<K, V, S, A> {
    ManagedLruCache {
        inner: EstimatedLruCache::unbounded_with_hasher_in(hasher, alloc),
        watermark_epoch,
        metrics_info: Some(metrics_info),
    }
}

pub fn new_with_hasher<K: Hash + Eq + EstimateSize, V: EstimateSize, S: BuildHasher>(
    watermark_epoch: Arc<AtomicU64>,
    metrics_info: MetricsInfo,
    hasher: S,
) -> ManagedLruCache<K, V, S> {
    ManagedLruCache {
        inner: EstimatedLruCache::unbounded_with_hasher(hasher),
        watermark_epoch,
        metrics_info: Some(metrics_info),
    }
}
