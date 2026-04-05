#![allow(dead_code)]

pub const NULL_IDX: u32 = u32::MAX;

#[repr(C)]
#[repr(align(32))]
#[derive(Debug, Clone, Copy)]
pub struct Order {
    pub order_id: u64,
    pub quantity: u32,
    pub next_idx: u32,
    pub prev_idx: u32,
}

pub struct OrderPool {
    pool: Vec<Order>,
    free_head: u32,
}

impl OrderPool {
    pub fn new(capacity: usize) -> Self {
        let mut pool = vec![
            Order {
                order_id: 0,
                quantity: 0,
                next_idx: NULL_IDX,
                prev_idx: NULL_IDX,
            };
            capacity
        ];

        for i in 0..(capacity - 1) {
            pool[i].next_idx = (i + 1) as u32;
        }

        Self { pool, free_head: 0 }
    }

    #[inline(always)]
    pub fn allocate(&mut self, order_id: u64, quantity: u32) -> u32 {
        let idx = self.free_head;
        if idx == NULL_IDX { panic!("Order pool exhausted!"); }
        
        self.free_head = self.pool[idx as usize].next_idx;
        
        let order = &mut self.pool[idx as usize];
        order.order_id = order_id;
        order.quantity = quantity;
        order.next_idx = NULL_IDX;
        order.prev_idx = NULL_IDX;
        
        idx
    }

    #[inline(always)]
    pub fn deallocate(&mut self, idx: u32) {
        let order = &mut self.pool[idx as usize];
        order.next_idx = self.free_head;
        self.free_head = idx;
    }

    #[inline(always)]
    pub unsafe fn get_mut_unchecked(&mut self, idx: u32) -> &mut Order {
        unsafe { self.pool.get_unchecked_mut(idx as usize) }
    }
}