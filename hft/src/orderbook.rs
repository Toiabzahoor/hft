#![allow(dead_code)]
use crate::pool::{OrderPool, NULL_IDX};

pub type Price = u64;
pub type Quantity = u32;

const TYPE_LIMIT: u8 = 0;
const TYPE_FOK: u8 = 2;

// NEW: The 16-byte Market Data Payload
#[derive(Default, Copy, Clone, Debug)]
pub struct MarketDataEvent {
    pub event_type: u8, // 1 = Trade Match, 2 = Best Bid/Ask Update
    pub ticker_id: u16,
    pub price: u64,
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
}

impl OrderBook {
    pub fn new(max_price_ticks: usize, max_orders: usize) -> Self {
        let mut bids = vec![PriceLevel::new(0); max_price_ticks];
        let mut asks = vec![PriceLevel::new(0); max_price_ticks];

        for (i, level) in bids.iter_mut().enumerate() { level.price = i as Price; }
        for (i, level) in asks.iter_mut().enumerate() { level.price = i as Price; }

        Self {
            bids, asks,
            best_bid: 0, best_ask: max_price_ticks as u64 - 1,
            order_map: vec![NULL_IDX; max_orders],
        }
    }

    #[inline(always)]
    pub fn can_fill_fully(&self, is_bid: bool, limit_price: Price, mut needed_qty: u32) -> bool {
        if is_bid {
            let mut current_price = self.best_ask;
            while current_price <= limit_price && current_price < self.asks.len() as u64 {
                let available = self.asks[current_price as usize].volume;
                if available >= needed_qty { return true; }
                needed_qty -= available;
                current_price += 1;
            }
        } else {
            let mut current_price = self.best_bid;
            while current_price >= limit_price && current_price > 0 {
                let available = self.bids[current_price as usize].volume;
                if available >= needed_qty { return true; }
                needed_qty -= available;
                current_price -= 1;
            }
        }
        false
    }

    // NEW: We pass down the ticker_id and the egress_buf to catch the generated events
    #[inline(always)]
    pub fn process_order(
        &mut self,
        pool: &mut OrderPool,
        is_bid: bool,
        price: Price,
        order_id: u64,
        mut quantity: u32,
        order_type: u8,
        ticker_id: u16, 
        egress_buf: &mut Vec<MarketDataEvent>,
    ) {
        if order_type == TYPE_FOK {
            if !self.can_fill_fully(is_bid, price, quantity) {
                return; 
            }
        }

        if is_bid {
            while quantity > 0 && price >= self.best_ask {
                quantity = self.execute_level(pool, false, self.best_ask, quantity, ticker_id, egress_buf);
                while self.asks[self.best_ask as usize].head_idx == NULL_IDX 
                      && self.best_ask < (self.asks.len() - 1) as u64 {
                    self.best_ask += 1;
                }
            }
        } else {
            while quantity > 0 && price <= self.best_bid && self.best_bid > 0 {
                quantity = self.execute_level(pool, true, self.best_bid, quantity, ticker_id, egress_buf);
                while self.bids[self.best_bid as usize].head_idx == NULL_IDX 
                      && self.best_bid > 0 {
                    self.best_bid -= 1;
                }
            }
        }

        if quantity > 0 && order_type == TYPE_LIMIT {
            self.add_order(pool, is_bid, price, order_id, quantity);
        }
    }

    #[inline(always)]
    pub fn add_order(
        &mut self, pool: &mut OrderPool, is_bid: bool, price: Price, order_id: u64, quantity: u32,
    ) -> u32 {
        let level = if is_bid { &mut self.bids[price as usize] } else { &mut self.asks[price as usize] };
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

        if is_bid && price > self.best_bid { self.best_bid = price; } 
        else if !is_bid && price < self.best_ask { self.best_ask = price; }

        self.order_map[order_id as usize] = new_idx;
        new_idx
    }

    #[inline(always)]
    pub fn cancel_order(&mut self, pool: &mut OrderPool, is_bid: bool, price: Price, order_id: u64) {
        let pool_idx = self.order_map[order_id as usize];
        if pool_idx == NULL_IDX { return; }

        let level = if is_bid { &mut self.bids[price as usize] } else { &mut self.asks[price as usize] };
        let (qty, prev_idx, next_idx) = unsafe {
            let order = pool.get_mut_unchecked(pool_idx);
            (order.quantity, order.prev_idx, order.next_idx)
        };

        unsafe {
            if prev_idx != NULL_IDX { pool.get_mut_unchecked(prev_idx).next_idx = next_idx; } 
            else { level.head_idx = next_idx; }

            if next_idx != NULL_IDX { pool.get_mut_unchecked(next_idx).prev_idx = prev_idx; } 
            else { level.tail_idx = prev_idx; }
        }

        level.volume -= qty;
        level.order_count -= 1;
        
        pool.deallocate(pool_idx);
        self.order_map[order_id as usize] = NULL_IDX; 
    }

    #[inline(always)]
    pub fn execute_level(
        &mut self, pool: &mut OrderPool, is_bid_level: bool, price: Price, mut aggressive_qty: u32,
        ticker_id: u16, egress_buf: &mut Vec<MarketDataEvent>
    ) -> u32 {
        let level = if is_bid_level { &mut self.bids[price as usize] } else { &mut self.asks[price as usize] };
        let mut current_idx = level.head_idx;

        while current_idx != NULL_IDX && aggressive_qty > 0 {
            let (qty, prev_idx, next_idx, o_id) = unsafe {
                let resting_order = pool.get_mut_unchecked(current_idx);
                (resting_order.quantity, resting_order.prev_idx, resting_order.next_idx, resting_order.order_id)
            };

            // NEW: Push the Trade Event directly to the local buffer (Zero-Allocation)
            let trade_qty = if qty <= aggressive_qty { qty } else { aggressive_qty };
            egress_buf.push(MarketDataEvent {
                event_type: 1, // Trade
                ticker_id,
                price,
                quantity: trade_qty,
            });

            if qty <= aggressive_qty {
                aggressive_qty -= qty;
                level.volume -= qty;
                level.order_count -= 1;

                unsafe {
                    if prev_idx != NULL_IDX { pool.get_mut_unchecked(prev_idx).next_idx = next_idx; } 
                    else { level.head_idx = next_idx; }

                    if next_idx != NULL_IDX { pool.get_mut_unchecked(next_idx).prev_idx = prev_idx; } 
                    else { level.tail_idx = prev_idx; }
                }

                pool.deallocate(current_idx);
                self.order_map[o_id as usize] = NULL_IDX; 
            } else {
                unsafe { pool.get_mut_unchecked(current_idx).quantity -= aggressive_qty; }
                level.volume -= aggressive_qty;
                aggressive_qty = 0;
            }
            current_idx = next_idx;
        }
        aggressive_qty
    }
}