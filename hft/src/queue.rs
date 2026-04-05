use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;

// 1. HARDWARE TRICK: Cache Line Padding
// We force this struct to occupy an entire 64-byte L1 cache line.
#[repr(C, align(64))]
struct CachePadded<T> {
    value: T,
}

pub struct SpscQueue<T> {
    buffer: Box<[UnsafeCell<T>]>,
    capacity: usize,
    mask: usize, // Used for insanely fast modulo math
    
    // The producer writes here, the consumer reads here
    head: CachePadded<AtomicUsize>, 
    
    // The consumer writes here, the producer reads here
    tail: CachePadded<AtomicUsize>, 
}

// We must manually tell Rust this is safe to share across threads 
// because we are using UnsafeCell.
unsafe impl<T: Send> Sync for SpscQueue<T> {}
unsafe impl<T: Send> Send for SpscQueue<T> {}

impl<T: Default + Copy> SpscQueue<T> {
    pub fn new(capacity: usize) -> Self {
        // 2. HARDWARE TRICK: Power of Two
        assert!(capacity.is_power_of_two(), "Capacity must be a power of 2");
        
        let mut vec = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            vec.push(UnsafeCell::new(T::default()));
        }
        
        Self {
            buffer: vec.into_boxed_slice(),
            capacity,
            mask: capacity - 1, // e.g., if cap is 1024, mask is 1023 (001111111111)
            head: CachePadded { value: AtomicUsize::new(0) },
            tail: CachePadded { value: AtomicUsize::new(0) },
        }
    }

    #[inline(always)]
    pub fn push(&self, item: T) -> Result<(), T> {
        let current_head = self.head.value.load(Ordering::Relaxed);
        let current_tail = self.tail.value.load(Ordering::Acquire);

        // If head has wrapped all the way around to catch up with tail, we are full.
        if current_head.wrapping_sub(current_tail) == self.capacity {
            return Err(item); 
        }

        // Bitwise AND is 10x faster than the modulo operator (%)
        let index = current_head & self.mask;
        
        unsafe {
            *self.buffer.get_unchecked(index).get() = item;
        }
        
        // 3. HARDWARE TRICK: Memory Ordering Fences
        self.head.value.store(current_head.wrapping_add(1), Ordering::Release);
        
        Ok(())
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        let current_tail = self.tail.value.load(Ordering::Relaxed);
        let current_head = self.head.value.load(Ordering::Acquire);

        if current_head == current_tail {
            return None; // Queue is completely empty
        }

        let index = current_tail & self.mask;
        
        let item = unsafe {
            *self.buffer.get_unchecked(index).get()
        };

        self.tail.value.store(current_tail.wrapping_add(1), Ordering::Release);
        
        Some(item)
    }
    /// Pushes a batch of items into the queue. Returns the number of items successfully pushed.
    #[inline(always)]
    pub fn push_batch(&self, items: &[T]) -> usize {
        let current_head = self.head.value.load(Ordering::Relaxed);
        let current_tail = self.tail.value.load(Ordering::Acquire);

        // Calculate how much space is actually left in the ring buffer
        let available = self.capacity - current_head.wrapping_sub(current_tail);
        let to_push = items.len().min(available);

        if to_push == 0 {
            return 0; // Queue is completely full
        }

        // Write all items to memory BEFORE updating the atomic head
        for i in 0..to_push {
            let index = current_head.wrapping_add(i) & self.mask;
            unsafe {
                *self.buffer.get_unchecked(index).get() = items[i];
            }
        }

        // A single Release fence for the entire batch
        self.head.value.store(current_head.wrapping_add(to_push), Ordering::Release);
        
        to_push
    }

    /// Pops a batch of items into a pre-allocated slice. Returns the number of items popped.
    #[inline(always)]
    pub fn pop_batch(&self, out: &mut [T]) -> usize {
        let current_tail = self.tail.value.load(Ordering::Relaxed);
        let current_head = self.head.value.load(Ordering::Acquire);

        // Calculate how many items are waiting to be read
        let available = current_head.wrapping_sub(current_tail);
        let to_pop = out.len().min(available);

        if to_pop == 0 {
            return 0; // Queue is completely empty
        }

        // Read all items from memory BEFORE updating the atomic tail
        for i in 0..to_pop {
            let index = current_tail.wrapping_add(i) & self.mask;
            out[i] = unsafe { *self.buffer.get_unchecked(index).get() };
        }

        // A single Release fence for the entire batch
        self.tail.value.store(current_tail.wrapping_add(to_pop), Ordering::Release);
        
        to_pop
    }
}