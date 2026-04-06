#![allow(dead_code)]
use crate::orderbook::{OrderBook, Price};
use crate::{OrderMessage, TYPE_LIMIT, TYPE_MODIFY};

pub struct MarketMakerStrategy {
    pub user_id: u16,
    pub active_bid_id: u64,
    pub active_ask_id: u64,
    pub current_bid_price: Price,
    pub current_ask_price: Price,
    pub base_order_id: u64,
    pub spread_tolerance: Price,
}

impl MarketMakerStrategy {
    pub fn new(user_id: u16, spread_tolerance: Price) -> Self {
        Self {
            user_id,
            active_bid_id: 0,
            active_ask_id: 0,
            current_bid_price: 0,
            current_ask_price: 0,
            // FIX: Start AI order IDs at 20,000. 
            // Retail maxes out around 19,531. Array maxes out at 50,000. This fits perfectly.
            base_order_id: 20_000, 
            spread_tolerance,
        }
    }

    #[inline(always)]
    pub fn on_book_update(&mut self, book: &OrderBook, ticker_id: u16) -> ([OrderMessage; 2], usize) {
        let mut actions = [OrderMessage::default(); 2];
        let mut count = 0;

        let best_bid = book.best_bid;
        let best_ask = book.best_ask;

        if best_bid == 0 || best_ask >= book.asks.len() as u64 || best_bid >= best_ask {
            return (actions, count);
        }

        let mid_price = (best_bid + best_ask) / 2;
        let target_bid = mid_price.saturating_sub(self.spread_tolerance);
        let target_ask = mid_price + self.spread_tolerance;

        if self.current_bid_price != target_bid {
            if self.active_bid_id != 0 {
                actions[count] = OrderMessage {
                    ticker_id, user_id: self.user_id, is_bid: true, order_type: TYPE_MODIFY,
                    price: target_bid, order_id: self.active_bid_id, quantity: 100, display_quantity: 100, stop_signal: false,
                };
            } else {
                self.base_order_id += 1;
                self.active_bid_id = self.base_order_id;
                actions[count] = OrderMessage {
                    ticker_id, user_id: self.user_id, is_bid: true, order_type: TYPE_LIMIT,
                    price: target_bid, order_id: self.active_bid_id, quantity: 100, display_quantity: 100, stop_signal: false,
                };
            }
            self.current_bid_price = target_bid;
            count += 1;
        }

        if self.current_ask_price != target_ask {
            if self.active_ask_id != 0 {
                actions[count] = OrderMessage {
                    ticker_id, user_id: self.user_id, is_bid: false, order_type: TYPE_MODIFY,
                    price: target_ask, order_id: self.active_ask_id, quantity: 100, display_quantity: 100, stop_signal: false,
                };
            } else {
                self.base_order_id += 1;
                self.active_ask_id = self.base_order_id;
                actions[count] = OrderMessage {
                    ticker_id, user_id: self.user_id, is_bid: false, order_type: TYPE_LIMIT,
                    price: target_ask, order_id: self.active_ask_id, quantity: 100, display_quantity: 100, stop_signal: false,
                };
            }
            self.current_ask_price = target_ask;
            count += 1;
        }

        (actions, count)
    }
}