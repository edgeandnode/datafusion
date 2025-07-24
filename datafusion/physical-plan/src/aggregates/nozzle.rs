use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, Result};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::GroupsAccumulatorAdapter;

use super::group_values::{new_group_values, GroupValues, GroupValuesRows};
use super::order::GroupOrdering;
use super::row_hash::create_group_accumulator;
use super::AggregateExec;

use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct AggregateState {
    inner: Arc<Mutex<AggregateStateInner>>,
}

enum AggregateStateInner {
    /// Standard aggregation mode
    Standard {
        /// An interning store of group keys that assigns consecutive group
        /// indices for each unique set of group values
        group_values: Box<dyn GroupValues>,

        /// Accumulators, one for each `AggregateFunctionExpr` in the query
        ///
        /// For example, if the query has aggregates, `SUM(x)`,
        /// `COUNT(y)`, there will be two accumulators, each one
        /// specialized for that particular aggregate and its input types
        accumulators: Vec<Box<dyn GroupsAccumulator>>,
    },
    
    /// Streaming aggregation mode with dual state
    Streaming {
        /// Permanent state - accumulates all history
        permanent_group_values: Box<GroupValuesRows>,
        permanent_accumulators: Vec<Box<GroupsAccumulatorAdapter>>,
        
        /// Current batch state - only changes in current batch
        current_group_values: Box<GroupValuesRows>,
        current_accumulators: Vec<Box<GroupsAccumulatorAdapter>>,
    },
}

impl std::fmt::Debug for AggregateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateState").finish()
    }
}

impl AggregateState {
    pub fn new(agg: &AggregateExec) -> Result<Self> {
        let group_schema = agg.group_by.group_schema(&agg.input.schema())?;
        let group_ordering = GroupOrdering::try_new(&agg.input_order_mode)?;
        let group_values = new_group_values(group_schema, &group_ordering)?;
        let accumulators: Vec<_> = agg
            .aggr_expr
            .iter()
            .map(create_group_accumulator)
            .collect::<Result<_>>()?;

        let inner = AggregateStateInner::Standard {
            group_values,
            accumulators,
        };

        Ok(AggregateState {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn supports_convert_to_state(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            AggregateStateInner::Standard { accumulators, .. } => {
                accumulators.iter().all(|acc| acc.supports_convert_to_state())
            }
            AggregateStateInner::Streaming { current_accumulators, .. } => {
                current_accumulators.iter().all(|acc| acc.supports_convert_to_state())
            }
        }
    }

    pub fn accumulators_len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            AggregateStateInner::Standard { accumulators, .. } => accumulators.len(),
            AggregateStateInner::Streaming { current_accumulators, .. } => current_accumulators.len(),
        }
    }

    pub fn group_values_len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            AggregateStateInner::Standard { group_values, .. } => group_values.len(),
            AggregateStateInner::Streaming { current_group_values, .. } => current_group_values.len(),
        }
    }

    pub fn group_values_is_empty(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            AggregateStateInner::Standard { group_values, .. } => group_values.is_empty(),
            AggregateStateInner::Streaming { current_group_values, .. } => current_group_values.is_empty(),
        }
    }

    pub fn group_values_intern(
        &self,
        cols: &[ArrayRef],
        group_indices: &mut Vec<usize>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            AggregateStateInner::Standard { group_values, .. } => {
                group_values.intern(cols, group_indices)
            }
            AggregateStateInner::Streaming { current_group_values, .. } => {
                current_group_values.intern(cols, group_indices)
            }
        }
    }

    pub fn with_accumulators<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Vec<Box<dyn GroupsAccumulator>>) -> Result<T>,
    {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            AggregateStateInner::Standard { accumulators, .. } => f(accumulators),
            AggregateStateInner::Streaming { .. } => {
                // For streaming mode, we don't support the generic accumulator access
                // Operations should use streaming-specific methods instead
                internal_err!("with_accumulators not supported in streaming mode")
            }
        }
    }

    pub fn accumulators_size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            AggregateStateInner::Standard { accumulators, .. } => {
                accumulators.iter().map(|x| x.size()).sum::<usize>()
            }
            AggregateStateInner::Streaming { current_accumulators, .. } => {
                current_accumulators.iter().map(|x| x.size()).sum::<usize>()
            }
        }
    }

    pub fn group_values_size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            AggregateStateInner::Standard { group_values, .. } => group_values.size(),
            AggregateStateInner::Streaming { current_group_values, .. } => current_group_values.size(),
        }
    }

    pub fn group_values_emit(&self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            AggregateStateInner::Standard { group_values, .. } => group_values.emit(emit_to),
            AggregateStateInner::Streaming { current_group_values, .. } => {
                current_group_values.emit(emit_to)
            }
        }
    }

    pub fn group_values_clear_shrink(&self, batch: &RecordBatch) {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            AggregateStateInner::Standard { group_values, .. } => {
                group_values.clear_shrink(batch);
            }
            AggregateStateInner::Streaming { current_group_values, .. } => {
                current_group_values.clear_shrink(batch);
            }
        }
    }
}
