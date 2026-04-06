#![allow(dead_code)]

pub const NULL_IDX: u32 = u32::MAX;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Order {
    pub order_id: u32,
    pub price: u32,
    pub quantity: u32,
    pub hidden_quantity: u32,
    pub display_clip: u32,
    pub next_idx: u32,
    pub prev_idx: u32,
    pub user_id: u16,
    pub is_bid: bool,
    pub _pad: u8,
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
                price: 0,
                quantity: 0,
                hidden_quantity: 0,
                display_clip: 0,
                next_idx: NULL_IDX,
                prev_idx: NULL_IDX,
                user_id: 0,
                is_bid: false,
                _pad: 0,
            };
            capacity
        ];

        for i in 0..(capacity - 1) {
            pool[i].next_idx = (i + 1) as u32;
        }

        Self { pool, free_head: 0 }
    }

    #[inline(always)]
    pub fn allocate(&mut self, order_id: u32, price: u32, is_bid: bool, quantity: u32, hidden_quantity: u32, display_clip: u32, user_id: u16) -> u32 {
        let idx = self.free_head;
        if idx == NULL_IDX { std::process::abort(); }
        
        self.free_head = self.pool[idx as usize].next_idx;
        
        let order = &mut self.pool[idx as usize];
        order.order_id = order_id;
        order.price = price;
        order.is_bid = is_bid;
        order.quantity = quantity;
        order.hidden_quantity = hidden_quantity;
        order.display_clip = display_clip;
        order.user_id = user_id;
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