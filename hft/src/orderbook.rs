use crate::pool::{OrderPool, NULL_IDX};

pub type Price = u64;
pub type Quantity = u32;

#[derive(Debug, Clone, Copy)]
pub struct PriceLevel {
    pub price: Price,
    pub volume: Quantity,
    pub order_count: u32,
    pub head_idx: u32,
    pub tail_idx: u32,
}

impl PriceLevel {
    pub fn new(price: Price) -> Self {
        Self {
            price,
            volume: 0,
            order_count: 0,
            head_idx: NULL_IDX,
            tail_idx: NULL_IDX,
        }
    }
}

pub struct OrderBook {
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
    pub best_bid: Price,
    pub best_ask: Price,
    // The O(1) lookup table: Index is Order ID, Value is Pool Index
    order_map: Vec<u32>, 
}

impl OrderBook {
    pub fn new(max_price_ticks: usize, max_orders: usize) -> Self {
        let mut bids = vec![PriceLevel::new(0); max_price_ticks];
        let mut asks = vec![PriceLevel::new(0); max_price_ticks];

        for (i, level) in bids.iter_mut().enumerate() { level.price = i as Price; }
        for (i, level) in asks.iter_mut().enumerate() { level.price = i as Price; }

        Self {
            bids,
            asks,
            best_bid: 0,
            best_ask: max_price_ticks as u64 - 1,
            // Pre-allocate the entire map and fill it with NULL_IDX
            order_map: vec![NULL_IDX; max_orders],
        }
    }

    #[inline(always)]
    pub fn add_order(
        &mut self,
        pool: &mut OrderPool,
        is_bid: bool,
        price: Price,
        order_id: u64,
        quantity: u32,
    ) -> u32 {
        let level = if is_bid {
            &mut self.bids[price as usize]
        } else {
            &mut self.asks[price as usize]
        };

        let new_idx = pool.allocate(order_id, quantity);

        unsafe {
            let new_order = pool.get_mut_unchecked(new_idx);

            if level.head_idx == NULL_IDX {
                level.head_idx = new_idx;
                level.tail_idx = new_idx;
            } else {
                new_order.prev_idx = level.tail_idx;
                let old_tail = pool.get_mut_unchecked(level.tail_idx);
                old_tail.next_idx = new_idx;
                level.tail_idx = new_idx;
            }
        }

        level.volume += quantity;
        level.order_count += 1;

        if is_bid && price > self.best_bid {
            self.best_bid = price;
        } else if !is_bid && price < self.best_ask {
            self.best_ask = price;
        }

        // Track the order in our O(1) map
        self.order_map[order_id as usize] = new_idx;

        new_idx
    }

    #[inline(always)]
    pub fn cancel_order(
        &mut self,
        pool: &mut OrderPool,
        is_bid: bool,
        price: Price,
        order_id: u64,
    ) {
        // 1. O(1) Lookup
        let pool_idx = self.order_map[order_id as usize];
        
        // If it's NULL_IDX, the order was already fully executed or canceled
        if pool_idx == NULL_IDX {
            return; 
        }

        let level = if is_bid {
            &mut self.bids[price as usize]
        } else {
            &mut self.asks[price as usize]
        };

        // Extract pointers into registers to satisfy borrow checker
        let (qty, prev_idx, next_idx) = unsafe {
            let order = pool.get_mut_unchecked(pool_idx);
            (order.quantity, order.prev_idx, order.next_idx)
        };

        // 2. O(1) Linked List Unlinking
        unsafe {
            if prev_idx != NULL_IDX {
                pool.get_mut_unchecked(prev_idx).next_idx = next_idx;
            } else {
                level.head_idx = next_idx; // We are removing the head
            }

            if next_idx != NULL_IDX {
                pool.get_mut_unchecked(next_idx).prev_idx = prev_idx;
            } else {
                level.tail_idx = prev_idx; // We are removing the tail
            }
        }

        // 3. Update Metrics & Free Memory
        level.volume -= qty;
        level.order_count -= 1;
        
        pool.deallocate(pool_idx);
        self.order_map[order_id as usize] = NULL_IDX; // Clear from map
    }

    #[inline(always)]
    pub fn execute_level(
        &mut self,
        pool: &mut OrderPool,
        is_bid_level: bool,
        price: Price,
        mut aggressive_qty: u32,
    ) -> u32 {
        let level = if is_bid_level {
            &mut self.bids[price as usize]
        } else {
            &mut self.asks[price as usize]
        };

        let mut current_idx = level.head_idx;

        while current_idx != NULL_IDX && aggressive_qty > 0 {
            // We also need the order_id here so we can remove it from the map if fully executed
            let (qty, prev_idx, next_idx, o_id) = unsafe {
                let resting_order = pool.get_mut_unchecked(current_idx);
                (resting_order.quantity, resting_order.prev_idx, resting_order.next_idx, resting_order.order_id)
            };

            if qty <= aggressive_qty {
                aggressive_qty -= qty;
                level.volume -= qty;
                level.order_count -= 1;

                unsafe {
                    if prev_idx != NULL_IDX {
                        pool.get_mut_unchecked(prev_idx).next_idx = next_idx;
                    } else {
                        level.head_idx = next_idx;
                    }

                    if next_idx != NULL_IDX {
                        pool.get_mut_unchecked(next_idx).prev_idx = prev_idx;
                    } else {
                        level.tail_idx = prev_idx;
                    }
                }

                pool.deallocate(current_idx);
                // Clear from the O(1) tracking map because the order no longer exists
                self.order_map[o_id as usize] = NULL_IDX; 
            } else {
                unsafe {
                    pool.get_mut_unchecked(current_idx).quantity -= aggressive_qty;
                }
                level.volume -= aggressive_qty;
                aggressive_qty = 0;
            }

            current_idx = next_idx;
        }

        aggressive_qty
    }
}