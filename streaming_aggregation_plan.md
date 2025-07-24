# Streaming Aggregation Implementation Plan

## Overview

This document outlines the plan for implementing streaming aggregation in DataFusion, building on the refactored `AggregateState` enum structure.

## Current Status

### Completed ✅

1. **Refactored AggregateState Structure**
   - Changed from a simple struct to an enum with two variants:
     - `Standard`: For normal aggregation operations
     - `Streaming`: For streaming aggregation with dual state
   - Maintained `Arc<Mutex>` wrapper for thread safety
   - Kept encapsulating method API for clean access

2. **Type-Safe Streaming Variant**
   ```rust
   enum AggregateStateInner {
       Standard {
           group_values: Box<dyn GroupValues>,
           accumulators: Vec<Box<dyn GroupsAccumulator>>,
       },
       Streaming {
           permanent_group_values: Box<GroupValuesRows>,
           permanent_accumulators: Vec<Box<GroupsAccumulatorAdapter>>,
           current_group_values: Box<GroupValuesRows>,
           current_accumulators: Vec<Box<GroupsAccumulatorAdapter>>,
       },
   }
   ```

## Remaining Implementation Tasks

### 1. Add Streaming-Specific Methods to GroupValuesRows

**Goal**: Enable efficient lookup and state transfer between permanent and current group values.

**Required Methods**:

```rust
impl GroupValuesRows {
    /// For streaming: intern values and find matches in permanent state
    pub fn intern_for_streaming(
        &mut self, 
        cols: &[ArrayRef], 
        groups: &mut Vec<usize>,
        permanent_rows: &GroupValuesRows,
    ) -> Result<HashMap<usize, usize>> {
        // 1. Normal intern into current
        // 2. For each new group in current, check if exists in permanent
        // 3. Return mapping: current_idx -> permanent_idx
    }
}
```

### 2. Add Streaming-Specific Methods to GroupsAccumulatorAdapter

**Goal**: Enable efficient state copying for individual groups.

**Required Methods**:

```rust
impl GroupsAccumulatorAdapter {
    /// Get a specific accumulator's reference
    pub fn get_accumulator_state(&self, group_idx: usize) -> Option<&dyn Accumulator>;
    
    /// Clone a specific accumulator's state
    pub fn clone_accumulator(&self, group_idx: usize) -> Result<Box<dyn Accumulator>>;
    
    /// Set accumulator at specific index
    pub fn set_accumulator(&mut self, group_idx: usize, accumulator: Box<dyn Accumulator>) -> Result<()>;
}
```

### 3. Implement Dual-State Management in AggregateState

**Goal**: Manage the lifecycle of streaming batches with current and permanent state.

**Required Methods**:

```rust
impl AggregateState {
    /// Initialize streaming mode from existing state
    pub fn new_streaming(/* params */) -> Result<Self>;
    
    /// Start processing a new streaming batch
    pub fn start_streaming_batch(&mut self) -> Result<()> {
        // Clear current state
        // Prepare for new data
    }
    
    /// Intern groups with streaming awareness
    pub fn intern_streaming(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // 1. Intern into current state
        // 2. Find matches in permanent state
        // 3. Copy accumulator state from permanent to current for existing groups
    }
    
    /// Update only current accumulators
    pub fn update_streaming(&mut self, /* params */) -> Result<()>;
}
```

### 4. Implement Commit and Evaluate Logic

**Goal**: Commit current state to permanent before emitting results.

**Key Design Decision**: Commit to permanent before emitting, so emission can safely consume current state.

**Required Methods**:

```rust
impl AggregateState {
    /// Merge current state into permanent state
    pub fn commit_current_to_permanent(&mut self) -> Result<()> {
        // For each group in current:
        // 1. Find or create in permanent
        // 2. Merge accumulator states
    }
    
    /// Evaluate and emit only current batch results
    pub fn evaluate_streaming(&mut self) -> Result<Vec<ArrayRef>> {
        // 1. Commit current to permanent
        // 2. Emit from current (consuming is OK now)
    }
}
```

### 5. Update GroupedHashAggregateStream

**Goal**: Add streaming mode support to the aggregate execution stream.

**Changes Required**:
- Add `streaming_mode` flag
- Implement `streaming_batch()` method
- Use streaming-specific methods when in streaming mode

## Streaming Execution Flow

1. **Start Batch**: 
   - Call `start_streaming_batch()`
   - Clear current state

2. **Process Data**:
   - Call `intern_streaming()` to handle groups
   - Call `update_streaming()` to update accumulators
   - Current state accumulates with initial values from permanent

3. **Commit**: 
   - Call `commit_current_to_permanent()`
   - Merge all current state into permanent

4. **Emit**: 
   - Call `evaluate_streaming()`
   - Return only current batch results
   - Current state can be safely consumed

## Key Design Principles

1. **Type Safety**: Use concrete types (`GroupValuesRows`, `GroupsAccumulatorAdapter`) in streaming mode
2. **Efficiency**: Only process/emit changed groups per batch
3. **State Preservation**: Permanent state accumulates all history
4. **Clean Separation**: Current vs permanent state are clearly separated
5. **Atomic Operations**: Commit-before-emit ensures consistency

## Benefits

- **Memory Efficient**: Current state only holds changed groups
- **Performance**: Only process what changed in each microbatch
- **Correct Semantics**: No ambiguity between null values and unchanged groups
- **Streaming Ready**: Designed for continuous processing with state preservation

## Next Steps

1. Implement the streaming-specific methods for `GroupValuesRows`
2. Implement the streaming-specific methods for `GroupsAccumulatorAdapter`
3. Add the streaming lifecycle methods to `AggregateState`
4. Implement the commit and evaluate logic
5. Update `GroupedHashAggregateStream` to support streaming mode
6. Add tests for streaming aggregation scenarios