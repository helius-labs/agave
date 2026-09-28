use {
    agave_scheduler_bindings::{
        SimulationResponseRegion, TransactionResponseRegion,
        worker_message_types::{
            self, CHECK_RESPONSE, CheckResponse, EXECUTION_RESPONSE, ExecutionResponse,
            SimulationResponse,
        },
    },
    rts_alloc::Allocator,
    std::ptr::NonNull,
};

/// Prepare a [`TransactionResponseRegion`] with [`ExecutionResponse`].
pub fn execution_responses_from_iter(
    allocator: &Allocator,
    iter: impl ExactSizeIterator<Item = ExecutionResponse>,
) -> Option<TransactionResponseRegion> {
    // SAFETY: EXECUTION_RESPONSE maps to ExecutionResponse.
    unsafe { from_iterator(allocator, EXECUTION_RESPONSE, iter) }
}

/// Prepare a [`SimulationResponseRegion`] with [`SimulationResponse`].
pub fn simulation_responses_from_iter(
    allocator: &Allocator,
    iter: impl ExactSizeIterator<Item = SimulationResponse>,
) -> Option<SimulationResponseRegion> {
    let num_transaction_responses = iter.len();
    let size = num_transaction_responses.wrapping_mul(core::mem::size_of::<SimulationResponse>());
    let response_ptr = allocator
        .allocate(size as u32)?
        .cast::<SimulationResponse>();
    debug_assert!(response_ptr.is_aligned());
    // SAFETY: this pointer was allocated from `allocator` above.
    let transaction_responses_offset = unsafe { allocator.offset(response_ptr.cast()) };
    for (index, response) in iter.enumerate() {
        // SAFETY: the allocation is sized for the exact iterator length.
        unsafe { response_ptr.add(index).write(response) };
    }

    Some(SimulationResponseRegion {
        num_transaction_responses: num_transaction_responses as u8,
        transaction_responses_offset,
    })
}

/// Prepare a [`TransactionResponseRegion`] with [`CheckResponse`].
pub fn resolve_responses_from_iter(
    allocator: &Allocator,
    iter: impl ExactSizeIterator<Item = CheckResponse>,
) -> Option<TransactionResponseRegion> {
    // SAFETY: CHECK_RESPONSE maps to CheckResponse.
    unsafe { from_iterator(allocator, CHECK_RESPONSE, iter) }
}

/// Allocate a tagged response region with [`CheckResponse`].
/// Each [`CheckResponse`] is not yet populated and must be populated by the
/// caller.
pub fn allocate_check_response_region(
    allocator: &Allocator,
    num_transaction_responses: usize,
) -> Option<(NonNull<CheckResponse>, TransactionResponseRegion)> {
    // SAFETY: CHECK_RESPONSE maps to CheckResponse.
    unsafe { allocate_response_region(allocator, CHECK_RESPONSE, num_transaction_responses) }
}

/// Allocate a response region.
///
/// # Safety
/// `tag` must describe `T`.
unsafe fn allocate_response_region<T: Sized>(
    allocator: &Allocator,
    tag: u8,
    num_transaction_responses: usize,
) -> Option<(NonNull<T>, TransactionResponseRegion)> {
    let size = num_transaction_responses.wrapping_mul(core::mem::size_of::<T>());
    let response_ptr = allocator.allocate(size as u32)?.cast::<T>();
    debug_assert!(
        response_ptr.is_aligned(),
        "allocator should guarantee alignment for the response types of interest"
    );

    // SAFETY: `response_ptr` was allocated from the allocator.
    let transaction_responses_offset = unsafe { allocator.offset(response_ptr.cast()) };

    Some((
        response_ptr,
        TransactionResponseRegion {
            tag,
            num_transaction_responses: num_transaction_responses as u8,
            transaction_responses_offset,
        },
    ))
}

unsafe fn from_iterator<T: Sized>(
    allocator: &Allocator,
    tag: u8,
    iter: impl ExactSizeIterator<Item = T>,
) -> Option<TransactionResponseRegion> {
    let (response_ptr, region) = unsafe { allocate_response_region(allocator, tag, iter.len())? };
    for (index, response) in iter.enumerate() {
        unsafe { response_ptr.add(index).write(response) };
    }
    Some(region)
}

#[derive(Debug)]
pub struct CheckResponsesPtr {
    ptr: NonNull<CheckResponse>,
    count: usize,
}

impl CheckResponsesPtr {
    /// Constructions a [`CheckResponsesPtr`] from raw parts.
    ///
    /// # Safety
    ///
    /// - `ptr` must be valid for reads.
    /// - `count` must be accurate (in number of responses) and not overrun the end of `ptr`.
    ///
    /// # Note
    ///
    /// If you are trying to construct a pointer for use by Agave, you almost certainly want to use
    /// [`Self::from_transaction_response_region`].
    pub unsafe fn from_raw_parts(ptr: NonNull<CheckResponse>, count: usize) -> Self {
        Self { ptr, count }
    }

    /// Constructs the pointer from a tagged [`TransactionResponseRegion`].
    ///
    /// # Safety
    ///
    /// - The allocation pointed to by this region must be valid and not previously freed.
    pub unsafe fn from_transaction_response_region(
        transaction_response_region: &TransactionResponseRegion,
        allocator: &Allocator,
    ) -> Self {
        debug_assert_eq!(
            transaction_response_region.tag,
            worker_message_types::CHECK_RESPONSE
        );
        Self {
            // SAFETY: `transaction_response_region.transaction_responses_offset` was allocated by `allocator`.
            ptr: unsafe {
                allocator.ptr_from_offset(transaction_response_region.transaction_responses_offset)
            }
            .cast(),
            count: transaction_response_region.num_transaction_responses as usize,
        }
    }

    /// The number of responses in this batch.
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Whether the batch is empty.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate the responses within the batch.
    pub fn iter(&self) -> impl Iterator<Item = &CheckResponse> {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }.iter()
    }

    /// Free the batch's allocation.
    ///
    /// # Safety
    ///
    /// - `Self` must be exclusively owned.
    pub unsafe fn free(self, allocator: &Allocator) {
        unsafe { allocator.free(self.ptr.cast()) }
    }
}

#[derive(Debug)]
pub struct ExecutionResponsesPtr {
    ptr: NonNull<ExecutionResponse>,
    count: usize,
}

impl ExecutionResponsesPtr {
    /// Constructions a [`ExecutionResponsesPtr`] from raw parts.
    ///
    /// # Safety
    ///
    /// - `ptr` must be valid for reads.
    /// - `count` must be accurate (in number of responses) and not overrun the end of `ptr`.
    ///
    /// # Note
    ///
    /// If you are trying to construct a pointer for use by Agave, you almost certainly want to use
    /// [`Self::from_transaction_response_region`].
    pub unsafe fn from_raw_parts(ptr: NonNull<ExecutionResponse>, count: usize) -> Self {
        Self { ptr, count }
    }

    /// Constructs the pointer from a tagged [`TransactionResponseRegion`].
    ///
    /// # Safety
    ///
    /// - The allocation pointed to by this region must be valid and not previously freed.
    pub unsafe fn from_transaction_response_region(
        transaction_response_region: &TransactionResponseRegion,
        allocator: &Allocator,
    ) -> Self {
        debug_assert_eq!(
            transaction_response_region.tag,
            worker_message_types::EXECUTION_RESPONSE
        );
        Self {
            // SAFETY: `transaction_response_region.transaction_responses_offset` was allocated by `allocator`.
            ptr: unsafe {
                allocator.ptr_from_offset(transaction_response_region.transaction_responses_offset)
            }
            .cast(),
            count: transaction_response_region.num_transaction_responses as usize,
        }
    }

    /// The number of responses in this batch.
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Whether the batch is empty.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate the responses within the batch.
    pub fn iter(&self) -> impl Iterator<Item = &ExecutionResponse> {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }.iter()
    }

    /// Free the batch's allocation.
    ///
    /// # Safety
    ///
    /// - `Self` must be exclusively owned.
    pub unsafe fn free(self, allocator: &Allocator) {
        unsafe { allocator.free(self.ptr.cast()) }
    }
}

#[derive(Debug)]
pub struct SimulationResponsesPtr {
    ptr: NonNull<SimulationResponse>,
    count: usize,
}

impl SimulationResponsesPtr {
    /// Constructions a [`SimulationResponsesPtr`] from raw parts.
    ///
    /// # Safety
    ///
    /// - `ptr` must be valid for reads.
    /// - `count` must be accurate (in number of responses) and not overrun the end of `ptr`.
    ///
    /// # Note
    ///
    /// If you are trying to construct a pointer for use by Agave, you almost certainly want to use
    /// [`Self::from_transaction_response_region`].
    pub unsafe fn from_raw_parts(ptr: NonNull<SimulationResponse>, count: usize) -> Self {
        Self { ptr, count }
    }

    /// Constructs the pointer from an [`SimulationResponseRegion`].
    ///
    /// # Safety
    ///
    /// - The allocation pointed to by this region must be valid and not previously freed.
    pub unsafe fn from_transaction_response_region(
        transaction_response_region: &SimulationResponseRegion,
        allocator: &Allocator,
    ) -> Self {
        Self {
            // SAFETY: `transaction_response_region.transaction_responses_offset` was allocated by `allocator`.
            ptr: unsafe {
                allocator.ptr_from_offset(transaction_response_region.transaction_responses_offset)
            }
            .cast(),
            count: transaction_response_region.num_transaction_responses as usize,
        }
    }

    /// The number of responses in this batch.
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Whether the batch is empty.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate the responses within the batch.
    pub fn iter(&self) -> impl Iterator<Item = &SimulationResponse> {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }.iter()
    }

    /// Free the batch's allocation.
    ///
    /// # Safety
    ///
    /// - `Self` must be exclusively owned.
    pub unsafe fn free(self, allocator: &Allocator) {
        unsafe { allocator.free(self.ptr.cast()) }
    }
}
