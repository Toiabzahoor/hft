#![allow(dead_code)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;

#[repr(C, align(64))]
struct CachePadded<T> {
    value: T,
}

pub struct SpscQueue<T> {
    buffer: Box<[UnsafeCell<T>]>,
    capacity: usize,
    mask: usize, 
    head: CachePadded<AtomicUsize>, 
    tail: CachePadded<AtomicUsize>, 
}

unsafe impl<T: Send> Sync for SpscQueue<T> {}
unsafe impl<T: Send> Send for SpscQueue<T> {}

impl<T: Default + Copy> SpscQueue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be a power of 2");
        
        let mut vec = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            vec.push(UnsafeCell::new(T::default()));
        }
        
        Self {
            buffer: vec.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
            head: CachePadded { value: AtomicUsize::new(0) },
            tail: CachePadded { value: AtomicUsize::new(0) },
        }
    }

    #[inline(always)]
    pub fn push(&self, item: T) -> Result<(), T> {
        let current_head = self.head.value.load(Ordering::Relaxed);
        let current_tail = self.tail.value.load(Ordering::Acquire);

        if current_head.wrapping_sub(current_tail) == self.capacity {
            return Err(item); 
        }

        let index = current_head & self.mask;
        unsafe { *self.buffer.get_unchecked(index).get() = item; }
        self.head.value.store(current_head.wrapping_add(1), Ordering::Release);
        
        Ok(())
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        let current_tail = self.tail.value.load(Ordering::Relaxed);
        let current_head = self.head.value.load(Ordering::Acquire);

        if current_head == current_tail {
            return None; 
        }

        let index = current_tail & self.mask;
        let item = unsafe { *self.buffer.get_unchecked(index).get() };
        self.tail.value.store(current_tail.wrapping_add(1), Ordering::Release);
        
        Some(item)
    }

    #[inline(always)]
    pub fn push_batch(&self, items: &[T]) -> usize {
        let current_head = self.head.value.load(Ordering::Relaxed);
        let current_tail = self.tail.value.load(Ordering::Acquire);

        let available = self.capacity - current_head.wrapping_sub(current_tail);
        let to_push = items.len().min(available);

        if to_push == 0 { return 0; }

        for i in 0..to_push {
            let index = current_head.wrapping_add(i) & self.mask;
            unsafe { *self.buffer.get_unchecked(index).get() = items[i]; }
        }

        self.head.value.store(current_head.wrapping_add(to_push), Ordering::Release);
        to_push
    }

    #[inline(always)]
    pub fn pop_batch(&self, out: &mut [T]) -> usize {
        let current_tail = self.tail.value.load(Ordering::Relaxed);
        let current_head = self.head.value.load(Ordering::Acquire);

        let available = current_head.wrapping_sub(current_tail);
        let to_pop = out.len().min(available);

        if to_pop == 0 { return 0; }

        for i in 0..to_pop {
            let index = current_tail.wrapping_add(i) & self.mask;
            out[i] = unsafe { *self.buffer.get_unchecked(index).get() };
        }

        self.tail.value.store(current_tail.wrapping_add(to_pop), Ordering::Release);
        to_pop
    }
}