#![allow(dead_code)]
use crate::pool::{OrderPool, NULL_IDX};

pub type Price = u32;
pub type Quantity = u32;

pub const TYPE_LIMIT: u8 = 0;
pub const TYPE_IOC: u8 = 1;
pub const TYPE_FOK: u8 = 2;
pub const TYPE_CANCEL: u8 = 3; 
pub const TYPE_MODIFY: u8 = 4; 

#[derive(Default, Copy, Clone, Debug)]
pub struct MarketDataEvent {
    pub event_type: u8, 
    pub ticker_id: u16,
    pub price: u32,
    pub quantity: u32,
}

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
        Self { price, volume: 0, order_count: 0, head_idx: NULL_IDX, tail_idx: NULL_IDX }
    }
}

pub struct OrderBook {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub best_bid: Price,
    pub best_ask: Price,
    order_map: Vec<u32>, 
    bid_bitmask: Vec<u64>,
    ask_bitmask: Vec<u64>,
}

impl OrderBook {
    pub fn new(max_price_ticks: usize, max_orders: usize) -> Self {
        let mut bids = vec![PriceLevel::new(0); max_price_ticks];
        let mut asks = vec![PriceLevel::new(0); max_price_ticks];

        for (i, level) in bids.iter_mut().enumerate() { level.price = i as Price; }
        for (i, level) in asks.iter_mut().enumerate() { level.price = i as Price; }

        let mask_len = (max_price_ticks / 64) + 1;

        Self {
            bids, asks,
            best_bid: 0, best_ask: u32::MAX, // Track empty asks with MAX
            order_map: vec![NULL_IDX; max_orders],
            bid_bitmask: vec![0; mask_len],
            ask_bitmask: vec![0; mask_len],
        }
    }

    #[inline(always)]
    fn set_bit(&mut self, is_bid: bool, price: u32) {
        let idx = (price / 64) as usize;
        let bit = price % 64;
        if is_bid { self.bid_bitmask[idx] |= 1u64 << bit; } 
        else { self.ask_bitmask[idx] |= 1u64 << bit; }
    }

    #[inline(always)]
    fn clear_bit(&mut self, is_bid: bool, price: u32) {
        let idx = (price / 64) as usize;
        let bit = price % 64;
        if is_bid { self.bid_bitmask[idx] &= !(1u64 << bit); } 
        else { self.ask_bitmask[idx] &= !(1u64 << bit); }
    }

    #[inline(always)]
    fn find_next_ask(&self, current: u32) -> u32 {
        if current == u32::MAX { return u32::MAX; }
        let mut idx = (current / 64) as usize;
        let bit = current % 64;
        let mut mask = self.ask_bitmask[idx] & !((1u64 << bit) - 1);

        while mask == 0 && idx < self.ask_bitmask.len() - 1 {
            idx += 1;
            mask = self.ask_bitmask[idx];
        }
        if mask == 0 { return u32::MAX; }
        (idx as u32 * 64) + mask.trailing_zeros()
    }

    #[inline(always)]
    fn find_next_bid(&self, current: u32) -> u32 {
        if current == 0 { return 0; }
        let mut idx = (current / 64) as usize;
        let bit = current % 64;
        
        // Safe left shift to avoid panic at 64 on the CPU
        let mut mask = if bit == 63 {
            self.bid_bitmask[idx]
        } else {
            self.bid_bitmask[idx] & ((1u64 << (bit + 1)) - 1)
        };

        while mask == 0 && idx > 0 {
            idx -= 1;
            mask = self.bid_bitmask[idx];
        }
        if mask == 0 { return 0; }
        (idx as u32 * 64) + (63 - mask.leading_zeros())
    }

    #[inline(always)]
    pub fn cancel_order(&mut self, pool: &mut OrderPool, order_id: u32, user_id: u16, ticker_id: u16, egress_buf: &mut Vec<MarketDataEvent>) {
        if (order_id as usize) >= self.order_map.len() { return; }
        let idx = self.order_map[order_id as usize];
        if idx == NULL_IDX { return; } 

        let (price, visible_qty, is_bid, owner_id, prev_idx, next_idx) = unsafe {
            let order = pool.get_mut_unchecked(idx);
            (order.price, order.quantity, order.is_bid, order.user_id, order.prev_idx, order.next_idx)
        };

        if owner_id != user_id { return; } 

        let is_empty = {
            let level = if is_bid { &mut self.bids[price as usize] } else { &mut self.asks[price as usize] };
            OrderBook::unlink_order(pool, level, prev_idx, next_idx, visible_qty);
            level.head_idx == NULL_IDX
        };

        if is_empty {
            self.clear_bit(is_bid, price);
            if is_bid && self.best_bid == price { self.best_bid = self.find_next_bid(price); }
            else if !is_bid && self.best_ask == price { self.best_ask = self.find_next_ask(price); }
        }

        pool.deallocate(idx);
        self.order_map[order_id as usize] = NULL_IDX;

        egress_buf.push(MarketDataEvent { event_type: 3, ticker_id, price, quantity: 0 });
    }

    #[inline(always)]
    pub fn modify_order(
        &mut self, pool: &mut OrderPool, order_id: u32, is_bid_payload: bool, new_price: Price, new_quantity: u32, 
        user_id: u16, ticker_id: u16, egress_buf: &mut Vec<MarketDataEvent>
    ) {
        if (order_id as usize) >= self.order_map.len() { return; }
        let idx = self.order_map[order_id as usize];
        
        if idx == NULL_IDX { 
            self.process_order(pool, is_bid_payload, new_price, order_id, new_quantity, new_quantity, TYPE_LIMIT, ticker_id, user_id, egress_buf);
            return; 
        }

        let (old_price, old_visible_qty, is_bid, owner_id) = unsafe {
            let order = pool.get_mut_unchecked(idx);
            (order.price, order.quantity, order.is_bid, order.user_id)
        };

        if owner_id != user_id { return; }

        if old_price == new_price && new_quantity <= old_visible_qty {
            let diff = old_visible_qty - new_quantity;
            let level = if is_bid { &mut self.bids[old_price as usize] } else { &mut self.asks[old_price as usize] };
            
            level.volume -= diff;
            unsafe { pool.get_mut_unchecked(idx).quantity = new_quantity; }
            egress_buf.push(MarketDataEvent { event_type: 4, ticker_id, price: new_price, quantity: new_quantity });
        } else {
            self.cancel_order(pool, order_id, user_id, ticker_id, egress_buf);
            self.process_order(pool, is_bid, new_price, order_id, new_quantity, new_quantity, TYPE_LIMIT, ticker_id, user_id, egress_buf);
        }
    }

    #[inline(always)]
    pub fn process_order(
        &mut self, pool: &mut OrderPool, is_bid: bool, price: Price, order_id: u32, 
        mut total_quantity: u32, display_quantity: u32, order_type: u8, 
        ticker_id: u16, user_id: u16, egress_buf: &mut Vec<MarketDataEvent>,
    ) {
        if is_bid {
            while total_quantity > 0 && self.best_ask != u32::MAX && price >= self.best_ask {
                total_quantity = self.execute_level(pool, false, self.best_ask, total_quantity, ticker_id, user_id, egress_buf);
                if self.asks[self.best_ask as usize].head_idx == NULL_IDX {
                    self.clear_bit(false, self.best_ask);
                    self.best_ask = self.find_next_ask(self.best_ask);
                }
            }
        } else {
            while total_quantity > 0 && self.best_bid > 0 && price <= self.best_bid {
                total_quantity = self.execute_level(pool, true, self.best_bid, total_quantity, ticker_id, user_id, egress_buf);
                if self.bids[self.best_bid as usize].head_idx == NULL_IDX {
                    self.clear_bit(true, self.best_bid);
                    self.best_bid = self.find_next_bid(self.best_bid);
                }
            }
        }

        if total_quantity > 0 && order_type == TYPE_LIMIT {
            let clip = if display_quantity == 0 || display_quantity >= total_quantity { total_quantity } else { display_quantity };
            let hidden = total_quantity - clip;
            self.add_order(pool, is_bid, price, order_id, clip, hidden, clip, user_id);
        }
    }

    #[inline(always)]
    pub fn add_order(
        &mut self, pool: &mut OrderPool, is_bid: bool, price: Price, order_id: u32, 
        visible_qty: u32, hidden_qty: u32, display_clip: u32, user_id: u16
    ) -> u32 {
        let is_empty = if is_bid { self.bids[price as usize].head_idx == NULL_IDX } else { self.asks[price as usize].head_idx == NULL_IDX };
        if is_empty { self.set_bit(is_bid, price); }

        let level = if is_bid { &mut self.bids[price as usize] } else { &mut self.asks[price as usize] };

        let new_idx = pool.allocate(order_id, price, is_bid, visible_qty, hidden_qty, display_clip, user_id);

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

        level.volume += visible_qty; 
        level.order_count += 1;

        if is_bid && price > self.best_bid { self.best_bid = price; } 
        else if !is_bid && (self.best_ask == u32::MAX || price < self.best_ask) { self.best_ask = price; }

        self.order_map[order_id as usize] = new_idx;
        new_idx
    }

    #[inline(always)]
    pub fn execute_level(
        &mut self, pool: &mut OrderPool, is_bid_level: bool, price: Price, mut aggressive_qty: u32,
        ticker_id: u16, user_id: u16, egress_buf: &mut Vec<MarketDataEvent>
    ) -> u32 {
        let level = if is_bid_level { &mut self.bids[price as usize] } else { &mut self.asks[price as usize] };
        let mut current_idx = level.head_idx;

        while current_idx != NULL_IDX && aggressive_qty > 0 {
            let (visible_qty, hidden_qty, display_clip, prev_idx, next_idx, o_id, resting_user_id) = unsafe {
                let order = pool.get_mut_unchecked(current_idx);
                (order.quantity, order.hidden_quantity, order.display_clip, order.prev_idx, order.next_idx, order.order_id, order.user_id)
            };

            if resting_user_id == user_id {
                OrderBook::unlink_order(pool, level, prev_idx, next_idx, visible_qty);
                pool.deallocate(current_idx);
                self.order_map[o_id as usize] = NULL_IDX; 
                current_idx = next_idx;
                continue; 
            }

            let trade_qty = if visible_qty <= aggressive_qty { visible_qty } else { aggressive_qty };
            egress_buf.push(MarketDataEvent { event_type: 1, ticker_id, price, quantity: trade_qty });

            if visible_qty <= aggressive_qty {
                aggressive_qty -= visible_qty;
                OrderBook::unlink_order(pool, level, prev_idx, next_idx, visible_qty);

                if hidden_qty > 0 {
                    let replenish = if hidden_qty > display_clip { display_clip } else { hidden_qty };
                    
                    unsafe {
                        let order = pool.get_mut_unchecked(current_idx);
                        order.quantity = replenish;
                        order.hidden_quantity -= replenish;
                        order.prev_idx = level.tail_idx;
                        order.next_idx = NULL_IDX;
                        
                        if level.tail_idx != NULL_IDX { pool.get_mut_unchecked(level.tail_idx).next_idx = current_idx; } 
                        else { level.head_idx = current_idx; }
                        level.tail_idx = current_idx;
                    }
                    level.volume += replenish;
                    level.order_count += 1;
                } else {
                    pool.deallocate(current_idx);
                    self.order_map[o_id as usize] = NULL_IDX; 
                }
            } else {
                unsafe { pool.get_mut_unchecked(current_idx).quantity -= aggressive_qty; }
                level.volume -= aggressive_qty;
                aggressive_qty = 0;
            }
            current_idx = next_idx;
        }
        aggressive_qty
    }

    #[inline(always)]
    fn unlink_order(pool: &mut OrderPool, level: &mut PriceLevel, prev_idx: u32, next_idx: u32, visible_qty: u32) {
        level.volume -= visible_qty;
        level.order_count -= 1;
        unsafe {
            if prev_idx != NULL_IDX { pool.get_mut_unchecked(prev_idx).next_idx = next_idx; } 
            else { level.head_idx = next_idx; }
            if next_idx != NULL_IDX { pool.get_mut_unchecked(next_idx).prev_idx = prev_idx; } 
            else { level.tail_idx = prev_idx; }
        }
    }
}