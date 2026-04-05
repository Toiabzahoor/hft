#![allow(dead_code)]
use crate::orderbook::OrderBook;

pub struct MarketMakerStrategy {
    pub current_position: i32, 
    pub max_position: i32,     
    pub base_spread: u64,      
}

#[derive(Debug)]
pub struct TargetQuote {
    pub bid: u64,
    pub ask: u64,
}

impl MarketMakerStrategy {
    pub fn new(max_position: i32, base_spread: u64) -> Self {
        Self { current_position: 0, max_position, base_spread }
    }

    #[inline(always)]
    pub fn on_book_update(&mut self, book: &OrderBook) -> Option<TargetQuote> {
        let best_bid = book.best_bid;
        let best_ask = book.best_ask;

        if best_bid == 0 || best_bid >= best_ask {
            return None; 
        }

        let mid_price = (best_bid as f64 + best_ask as f64) / 2.0;

        let bid_vol = book.bids[best_bid as usize].volume as f64;
        let ask_vol = book.asks[best_ask as usize].volume as f64;
        let total_vol = bid_vol + ask_vol;
        
        let obi = if total_vol > 0.0 { (bid_vol - ask_vol) / total_vol } else { 0.0 };
        let inventory_skew = self.current_position as f64 / self.max_position as f64;
        let skew_ticks = (obi * 2.0) - (inventory_skew * 2.0);
        let target_mid = (mid_price + skew_ticks).round() as u64;

        Some(TargetQuote {
            bid: target_mid.saturating_sub(self.base_spread / 2),
            ask: target_mid + (self.base_spread / 2),
        })
    }
}