// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use log::trace;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc, Schema};
use risingwave_common::error::RwError;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::{next_key, range_of_prefix};

use super::mem_table::RowOp;
use crate::cell_based_row_deserializer::ColumnDescMapping;
use crate::error::{StorageError, StorageResult};
use crate::keyspace::StripPrefixIterator;
use crate::row_based_deserializer::RowBasedDeserializer;
use crate::row_based_serializer::RowBasedSerializer;
use crate::storage_value::{StorageValue, ValueMeta};
use crate::{Keyspace, StateStore, StateStoreIter};

/// `RowBasedTable` is the interface accessing relational data in KV(`StateStore`) with encoding
/// format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
#[derive(Clone)]
pub struct RowBasedTable<S: StateStore> {
    /// The keyspace that the pk and value of the original table has.
    keyspace: Keyspace<S>,

    /// All columns of this table. Note that this is different from the output columns in
    /// `mapping.output_columns`.
    #[allow(dead_code)]
    table_columns: Vec<ColumnDesc>,

    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// RowSeqScanExecutor.
    schema: Schema,

    /// Used for serializing the primary key.
    pk_serializer: OrderedRowSerializer,

    /// Used for serializing the row.
    row_based_deserialize: RowBasedDeserializer,

    row_based_serialize: RowBasedSerializer,

    /// Mapping from column id to column index. Used for deserializing the row.
    mapping: Arc<ColumnDescMapping>,

    /// Indices of primary keys.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.

    /// Indices of distribution keys for computing vnode. None if vnode falls to default value.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    dist_key_indices: Option<Vec<usize>>,
}

impl<S: StateStore> std::fmt::Debug for RowBasedTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowBasedTable").finish_non_exhaustive()
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::RowBasedTable(rw.into())
}

impl<S: StateStore> RowBasedTable<S> {
    /// Create a [`RowBasedTable`] given a complete set of `columns`.
    pub fn new(
        keyspace: Keyspace<S>,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        dist_key_indices: Option<Vec<usize>>,
    ) -> Self {
        let column_ids = columns.iter().map(|c| c.column_id).collect();

        Self::new_partial(
            keyspace,
            columns,
            column_ids,
            order_types,
            pk_indices,
            dist_key_indices,
        )
    }

    /// Create a [`RowBasedTable`] given a complete set of `columns` and a partial set of
    /// `column_ids`. The output will only contains columns with the given ids in the same order.
    pub fn new_partial(
        keyspace: Keyspace<S>,
        table_columns: Vec<ColumnDesc>,
        column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        dist_key_indices: Option<Vec<usize>>,
    ) -> Self {
        let mapping = ColumnDescMapping::new_partial(&table_columns, &column_ids);
        let schema = Schema::new(mapping.output_columns.iter().map(Into::into).collect());
        let pk_serializer = OrderedRowSerializer::new(order_types);
        let data_types = table_columns
            .clone()
            .into_iter()
            .map(|t| t.data_type)
            .collect_vec();
        Self {
            keyspace,
            table_columns,
            schema,
            pk_serializer,
            row_based_deserialize: RowBasedDeserializer::new(data_types),
            row_based_serialize: RowBasedSerializer::new(),
            mapping,
            dist_key_indices,
        }
    }



    pub async fn get_row(&mut self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        let serialized_pk = serialize_pk(pk, &self.pk_serializer);

        match self.keyspace.get(&serialized_pk, epoch).await? {
            Some(value) => {
                let row = self
                    .row_based_deserialize
                    .deserialize(value.to_vec())
                    .map_err(err)?;
                return Ok(Some(row));
            }
            None => return Ok(None),
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns whether the output columns are a complete set of the table's.
    fn is_complete(&self) -> bool {
        use std::collections::HashSet;

        let output: HashSet<_> = self
            .mapping
            .output_columns
            .iter()
            .map(|c| c.column_id)
            .collect();
        let table: HashSet<_> = self.table_columns.iter().map(|c| c.column_id).collect();

        output == table
    }

    fn compute_vnode_by_row(&self, value: &Row) -> u16 {
        let dist_key_indices = self.dist_key_indices.as_ref().unwrap();

        let hash_builder = CRC32FastBuilder {};
        value
            .hash_by_indices(dist_key_indices, &hash_builder)
            .unwrap()
            .to_vnode()
    }
}

/// Get & Write
impl<S: StateStore> RowBasedTable<S> {
    async fn batch_write_rows_inner<const WITH_VALUE_META: bool>(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        debug_assert!(self.is_complete(), "cannot write to a partial table");

        // stateful executors need to compute vnode.
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);

        for (pk, row_op) in buffer {
            // If value meta is computed here, then the Row based table is guaranteed to have
            // distribution keys. Also, it is guaranteed that distribution key indices will
            // not exceed the length of pk. So we simply do unwrap here.
            match row_op {
                RowOp::Insert(row) => {
                    let value_meta = if WITH_VALUE_META {
                        ValueMeta::with_vnode(self.compute_vnode_by_row(&row))
                    } else {
                        ValueMeta::default()
                    };
                    let value = self.row_based_serialize.serialize(&row).map_err(err)?;
                    local.put(pk, StorageValue::new_put(value_meta, value))
                }
                RowOp::Delete(old_row) => {
                    // TODO(wcy-fdu): only serialize key on deletion
                    let value_meta = if WITH_VALUE_META {
                        ValueMeta::with_vnode(self.compute_vnode_by_row(&old_row))
                    } else {
                        ValueMeta::default()
                    };
                    local.delete_with_value_meta(pk, value_meta);
                }
                RowOp::Update((_, new_row)) => {
                    let value_meta = if WITH_VALUE_META {
                        ValueMeta::with_vnode(self.compute_vnode_by_row(&new_row))
                    } else {
                        ValueMeta::default()
                    };
                    // local.delete_with_value_meta(pk.clone(), value_meta);
                    let insert_value = self.row_based_serialize.serialize(&new_row).map_err(err)?;
                    local.put(pk, StorageValue::new_put(value_meta, insert_value))
                }
            }
        }
        batch.ingest(epoch).await?;
        Ok(())
    }

    /// Write to state store, and use distribution key indices to compute value meta
    pub async fn batch_write_rows_with_value_meta(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        self.batch_write_rows_inner::<true>(buffer, epoch).await
    }

    /// Write to state store without value meta
    pub async fn batch_write_rows(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        self.batch_write_rows_inner::<false>(buffer, epoch).await
    }
}

pub trait PkAndRowStream = Stream<Item = StorageResult<(Vec<u8>, Row)>> + Send;

/// The [`RowBasedIter`] used in streaming executor.
pub type StreamingIter<S: StateStore> = impl PkAndRowStream;
/// The [`RowBasedIter`] used in batch executor, which will wait for the epoch before iteration.
pub type BatchIter<S: StateStore> = impl PkAndRowStream;
/// The [`RowBasedIter`] used in batch executor if pk is not persisted, which will wait for the
/// epoch before iteration.
pub type BatchDedupPkIter<S: StateStore> = impl PkAndRowStream;
// #[async_trait::async_trait]
// impl<S: PkAndRowStream + Unpin> TableIter for S {
//     async fn next_row(&mut self) -> StorageResult<Option<Row>> {
//         self.next()
//             .await
//             .transpose()
//             .map(|r| r.map(|(_pk, row)| row))
//     }
// }

/// Iterators
impl<S: StateStore> RowBasedTable<S> {
    /// Get a [`StreamingIter`] with given `encoded_key_range`.
    pub(super) async fn streaming_iter_with_encoded_key_range<R, B>(
        &self,
        encoded_key_range: R,
        epoch: u64,
    ) -> StorageResult<StreamingIter<S>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        let data_types = self
            .table_columns
            .clone()
            .into_iter()
            .map(|t| t.data_type)
            .collect_vec();
        Ok(RowBasedIter::<_, STREAMING_ITER_TYPE>::new(
            &self.keyspace,
            data_types,
            encoded_key_range,
            epoch,
        )
        .await?
        .into_stream())
    }

    /// Get a [`BatchIter`] with given `encoded_key_range`.
    pub(super) async fn batch_iter_with_encoded_key_range<R, B>(
        &self,
        encoded_key_range: R,
        epoch: u64,
    ) -> StorageResult<BatchIter<S>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        let data_types = self
            .table_columns
            .clone()
            .into_iter()
            .map(|t| t.data_type)
            .collect_vec();
        Ok(RowBasedIter::<_, BATCH_ITER_TYPE>::new(
            &self.keyspace,
            data_types,
            encoded_key_range,
            epoch,
        )
        .await?
        .into_stream())
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    pub async fn batch_iter(&self, epoch: u64) -> StorageResult<BatchIter<S>> {
        self.batch_iter_with_encoded_key_range::<_, &[u8]>(.., epoch)
            .await
    }

    /// `dedup_pk_iter` should be used when pk is not persisted as value in storage.
    /// It will attempt to decode pk from key instead of Row value.
    /// Tracking issue: <https://github.com/singularity-data/risingwave/issues/588>
    pub async fn batch_dedup_pk_iter(
        &self,
        epoch: u64,
        // TODO: remove this parameter: https://github.com/singularity-data/risingwave/issues/3203
        pk_descs: &[OrderedColumnDesc],
    ) -> StorageResult<BatchDedupPkIter<S>> {
        Ok(DedupPkRowBasedIter::new(
            self.batch_iter(epoch).await?,
            self.mapping.clone(),
            pk_descs,
        )
        .await?
        .into_stream())
    }

    pub async fn batch_iter_with_pk_bounds(
        &self,
        epoch: u64,
        pk_prefix: Row,
        next_col_bounds: impl RangeBounds<Datum>,
    ) -> StorageResult<BatchIter<S>> {
        fn serialize_pk_bound(
            pk_serializer: &OrderedRowSerializer,
            pk_prefix: &Row,
            next_col_bound: Bound<&Datum>,
            is_start_bound: bool,
        ) -> Bound<Vec<u8>> {
            match next_col_bound {
                Included(k) => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size() + 1);
                    let mut key = pk_prefix.clone();
                    key.0.push(k.clone());
                    let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                    if is_start_bound {
                        Included(serialized_key)
                    } else {
                        // Should use excluded next key for end bound.
                        // Otherwise keys starting with the bound is not included.
                        Excluded(next_key(&serialized_key))
                    }
                }
                Excluded(k) => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size() + 1);
                    let mut key = pk_prefix.clone();
                    key.0.push(k.clone());
                    let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                    if is_start_bound {
                        // storage doesn't support excluded begin key yet, so transform it to
                        // included
                        Included(next_key(&serialized_key))
                    } else {
                        Excluded(serialized_key)
                    }
                }
                Unbounded => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size());
                    let serialized_pk_prefix = serialize_pk(pk_prefix, &pk_prefix_serializer);
                    if pk_prefix.size() == 0 {
                        Unbounded
                    } else if is_start_bound {
                        Included(serialized_pk_prefix)
                    } else {
                        Excluded(next_key(&serialized_pk_prefix))
                    }
                }
            }
        }

        let start_key = serialize_pk_bound(
            &self.pk_serializer,
            &pk_prefix,
            next_col_bounds.start_bound(),
            true,
        );
        let end_key = serialize_pk_bound(
            &self.pk_serializer,
            &pk_prefix,
            next_col_bounds.end_bound(),
            false,
        );

        trace!(
            "iter_with_pk_bounds: start_key: {:?}, end_key: {:?}",
            start_key,
            end_key
        );

        self.batch_iter_with_encoded_key_range((start_key, end_key), epoch)
            .await
    }

    pub async fn batch_iter_with_pk_prefix(
        &self,
        epoch: u64,
        pk_prefix: Row,
    ) -> StorageResult<BatchIter<S>> {
        let prefix_serializer = self.pk_serializer.prefix(pk_prefix.size());
        let serialized_pk_prefix = serialize_pk(&pk_prefix, &prefix_serializer);

        let key_range = range_of_prefix(&serialized_pk_prefix);

        trace!(
            "iter_with_pk_prefix: key_range {:?}",
            (key_range.start_bound(), key_range.end_bound())
        );

        self.batch_iter_with_encoded_key_range(key_range, epoch)
            .await
    }
}

const STREAMING_ITER_TYPE: bool = true;
const BATCH_ITER_TYPE: bool = false;

/// [`RowBasedIter`] iterates on the Row-based table.
/// If `ITER_TYPE` is `BATCH`, it will wait for the given epoch to be committed before iteration.
struct RowBasedIter<S: StateStore, const ITER_TYPE: bool> {
    /// An iterator that returns raw bytes from storage.
    iter: StripPrefixIterator<S::Iter>,

    /// Row-based row deserializer
    row_based_deserializer: RowBasedDeserializer,
}

impl<S: StateStore, const ITER_TYPE: bool> RowBasedIter<S, ITER_TYPE> {
    async fn new<R, B>(
        keyspace: &Keyspace<S>,
        data_types: Vec<DataType>,
        encoded_key_range: R,
        epoch: u64,
    ) -> StorageResult<Self>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        if ITER_TYPE == BATCH_ITER_TYPE {
            keyspace.state_store().wait_epoch(epoch).await?;
        }
        let row_based_deserializer = RowBasedDeserializer::new(data_types);

        let iter = keyspace.iter_with_range(encoded_key_range, epoch).await?;
        let iter = Self {
            iter,
            row_based_deserializer,
        };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(mut self) {
        while let Some((key, value)) = self.iter.next().await? {
            let pk_and_row = self
                .row_based_deserializer
                .deserialize(value.to_vec())
                .map_err(err)?;

            yield (key.to_vec(), pk_and_row);
        }
    }
}

struct DedupPkRowBasedIter<I> {
    inner: I,
    pk_decoder: OrderedRowDeserializer,

    // Maps pk fields with:
    // 1. same value and memcomparable encoding,
    // 2. corresponding row positions. e.g. _row_id is unlikely to be part of selected row.
    pk_to_row_mapping: Vec<Option<usize>>,
}

impl<I> DedupPkRowBasedIter<I> {
    async fn new(
        inner: I,
        mapping: Arc<ColumnDescMapping>,
        pk_descs: &[OrderedColumnDesc],
    ) -> StorageResult<Self> {
        let (data_types, order_types) = pk_descs
            .iter()
            .map(|ordered_desc| {
                (
                    ordered_desc.column_desc.data_type.clone(),
                    ordered_desc.order,
                )
            })
            .unzip();
        let pk_decoder = OrderedRowDeserializer::new(data_types, order_types);

        let pk_to_row_mapping = pk_descs
            .iter()
            .map(|d| {
                let column_desc = &d.column_desc;
                if column_desc.data_type.mem_cmp_eq_value_enc() {
                    mapping.get(column_desc.column_id).map(|(_, index)| index)
                } else {
                    None
                }
            })
            .collect();

        Ok(Self {
            inner,
            pk_decoder,
            pk_to_row_mapping,
        })
    }
}

impl<I: PkAndRowStream> DedupPkRowBasedIter<I> {
    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(self) {
        #[for_await]
        for r in self.inner {
            let (pk_vec, Row(mut row_inner)) = r?;
            let pk_decoded = self.pk_decoder.deserialize(&pk_vec).map_err(err)?;
            for (pk_idx, datum) in pk_decoded.into_vec().into_iter().enumerate() {
                if let Some(row_idx) = self.pk_to_row_mapping[pk_idx] {
                    row_inner[row_idx] = datum;
                }
            }
            yield (pk_vec, Row(row_inner));
        }
    }
}
