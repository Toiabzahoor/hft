mod pool;
mod orderbook;
mod queue; 
mod network;
mod strategy;
mod risk; 
mod journal; 

use pool::OrderPool;
use orderbook::{OrderBook, MarketDataEvent, TYPE_LIMIT, TYPE_IOC, TYPE_FOK, TYPE_CANCEL, TYPE_MODIFY};
use queue::SpscQueue;
use strategy::MarketMakerStrategy; 
use risk::RiskGateway; 
use journal::SequencerJournal; 
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::env;

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

#[repr(C)] 
#[derive(Default, Copy, Clone, Debug)]
pub struct OrderMessage {
    pub ticker_id: u16, 
    pub user_id: u16, 
    pub is_bid: bool,
    pub order_type: u8, 
    pub price: u64,
    pub order_id: u64,
    pub quantity: u32,
    pub display_quantity: u32, 
    pub stop_signal: bool,
}

pub struct Exchange {
    books: Vec<OrderBook>,
    pools: Vec<OrderPool>,
    strategies: Vec<MarketMakerStrategy>,
    pub egress_queue: Arc<SpscQueue<MarketDataEvent>>,
    pub egress_buffer: Vec<MarketDataEvent>, 
}

impl Exchange {
    pub fn new(num_tickers: usize, pool_capacity_per_ticker: usize, egress_queue: Arc<SpscQueue<MarketDataEvent>>) -> Self {
        println!("Allocating Exchange Memory for {} parallel tickers...", num_tickers);
        
        let mut books = Vec::with_capacity(num_tickers);
        let mut pools = Vec::with_capacity(num_tickers);
        let mut strategies = Vec::with_capacity(num_tickers);

        for _ in 0..num_tickers {
            books.push(OrderBook::new(100_000, pool_capacity_per_ticker));
            pools.push(OrderPool::new(pool_capacity_per_ticker));
            strategies.push(MarketMakerStrategy::new(1000, 4));
        }

        Self { books, pools, strategies, egress_queue, egress_buffer: Vec::with_capacity(1024) }
    }

    #[inline(always)]
    pub fn prefetch_memory(&self, msg: &OrderMessage) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let idx = msg.ticker_id as usize;
            let pool_ptr = self.pools.get_unchecked(idx) as *const _ as *const i8;
            _mm_prefetch(pool_ptr, _MM_HINT_T0);

            let book = self.books.get_unchecked(idx);
            let level_ptr = if msg.is_bid {
                book.bids.as_ptr().add(msg.price as usize) as *const i8
            } else {
                book.asks.as_ptr().add(msg.price as usize) as *const i8
            };
            _mm_prefetch(level_ptr, _MM_HINT_T0);
        }
    }

    // --- NEW: Flattened Architecture (Zero Memset Overhead) ---
    #[inline(always)]
    pub fn route_and_execute(&mut self, msg: OrderMessage) {
        let idx = msg.ticker_id as usize;
        
        // Step 1: Process the Retail Order & Evaluate the AI
        let (ai_actions, ai_count) = unsafe {
            let book = self.books.get_unchecked_mut(idx);
            let pool = self.pools.get_unchecked_mut(idx);
            let strategy = self.strategies.get_unchecked_mut(idx);

            // Highly predictable branch logic
            if msg.order_type <= TYPE_FOK {
                book.process_order(pool, msg.is_bid, msg.price, msg.order_id, msg.quantity, msg.display_quantity, msg.order_type, msg.ticker_id, msg.user_id, &mut self.egress_buffer);
            } else if msg.order_type == TYPE_MODIFY {
                book.modify_order(pool, msg.order_id, msg.is_bid, msg.price, msg.quantity, msg.user_id, msg.ticker_id, &mut self.egress_buffer);
            } else {
                book.cancel_order(pool, msg.order_id, msg.user_id, msg.ticker_id, &mut self.egress_buffer);
            }

            strategy.on_book_update(book, msg.ticker_id)
        };

        // Step 2: Process the AI Responses Instantly
        // We do NOT feed these back into the strategy to prevent recursive unrolling.
        if ai_count > 0 {
            unsafe {
                let book = self.books.get_unchecked_mut(idx);
                let pool = self.pools.get_unchecked_mut(idx);

                for i in 0..ai_count {
                    let ai_msg = &ai_actions[i];
                    
                    if ai_msg.order_type <= TYPE_FOK {
                        book.process_order(pool, ai_msg.is_bid, ai_msg.price, ai_msg.order_id, ai_msg.quantity, ai_msg.display_quantity, ai_msg.order_type, ai_msg.ticker_id, ai_msg.user_id, &mut self.egress_buffer);
                    } else if ai_msg.order_type == TYPE_MODIFY {
                        book.modify_order(pool, ai_msg.order_id, ai_msg.is_bid, ai_msg.price, ai_msg.quantity, ai_msg.user_id, ai_msg.ticker_id, &mut self.egress_buffer);
                    } else {
                        book.cancel_order(pool, ai_msg.order_id, ai_msg.user_id, ai_msg.ticker_id, &mut self.egress_buffer);
                    }
                }
            }
        }
    }
}

fn run_replay(journal_path: &str) {
    println!("Initiating Crash Recovery: Replaying Journal from disk...");
    
    let file = std::fs::File::open(journal_path).expect("Failed to open journal.");
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file).expect("Failed to map memory") };

    let msg_size = std::mem::size_of::<OrderMessage>();
    let num_messages = mmap.len() / msg_size;
    println!("Located {} exact frozen orders in the journal.", num_messages);

    let start = Instant::now();
    let ptr = mmap.as_ptr() as *const OrderMessage;
    
    let mut cancel_count = 0;
    let mut modify_count = 0;

    for i in 0..num_messages {
        let msg = unsafe { *ptr.add(i) }; 
        if msg.order_type == TYPE_CANCEL { cancel_count += 1; }
        if msg.order_type == TYPE_MODIFY { modify_count += 1; }
    }

    println!("--- RECOVERY SCAN COMPLETE ---");
    println!("Scanned {} million orders in {:?}", num_messages / 1_000_000, start.elapsed());
    println!("Discovered {} Cancels and {} Modifications.", cancel_count, modify_count);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 && args[1] == "replay" {
        run_replay("market_events.journal");
        return;
    }

    println!("Initializing Complete Architecture: Ingress -> Engine -> Egress...");

    let ingress_queue = Arc::new(SpscQueue::<OrderMessage>::new(131_072));
    let egress_queue = Arc::new(SpscQueue::<MarketDataEvent>::new(131_072));

    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let network_core = core_ids.first().copied();
    let engine_core = core_ids.get(1).copied().or(network_core);
    let publisher_core = core_ids.last().copied().or(engine_core);

    let total_messages = 5_000_000; 
    let num_tickers = 256; 

    let publisher_queue = Arc::clone(&egress_queue);
    let _publisher_handle = thread::spawn(move || {
        if let Some(id) = publisher_core { core_affinity::set_for_current(id); }
        let mut local_batch = [MarketDataEvent::default(); 128];
        loop {
            let popped = publisher_queue.pop_batch(&mut local_batch);
            if popped == 0 { std::hint::spin_loop(); }
        }
    });

    let consumer_queue = Arc::clone(&ingress_queue);
    let consumer_handle = thread::spawn(move || {
        if let Some(id) = engine_core { core_affinity::set_for_current(id); }

        let mut exchange = Exchange::new(num_tickers, 50_000, egress_queue);
        let mut risk_gateway = RiskGateway::new(10_000); 
        let mut local_batch = [OrderMessage::default(); 32]; 
        
        let mut start_time = Instant::now();
        let mut is_timing = false;
        let mut processed_count = 0; 

        loop {
            let popped = consumer_queue.pop_batch(&mut local_batch);
            
            if popped > 0 {
                exchange.egress_buffer.clear();

                for i in 0..popped {
                    let msg = local_batch[i];
                    
                    if processed_count == 0 && !is_timing {
                        start_time = Instant::now();
                        is_timing = true;
                    }

                    if risk_gateway.check_pre_trade(&msg) {
                        let lookahead = 2; 
                        if i + lookahead < popped {
                            exchange.prefetch_memory(&local_batch[i + lookahead]);
                        }
                        exchange.route_and_execute(msg);
                    }

                    processed_count += 1;

                    if processed_count == total_messages && is_timing {
                        let elapsed = start_time.elapsed();
                        let nanos_per_msg = elapsed.as_nanos() as f64 / total_messages as f64;
                        
                        println!("--- PIPELINE BENCHMARK COMPLETE ---");
                        println!("Processed {} packets across {} tickers in {:?}", total_messages, num_tickers, elapsed);
                        println!("End-to-End Latency: {:.2} nanoseconds per message", nanos_per_msg);
                        std::process::exit(0); 
                    }
                }

                if !exchange.egress_buffer.is_empty() {
                    exchange.egress_queue.push_batch(&exchange.egress_buffer);
                }

            } else {
                std::hint::spin_loop(); 
            }
        }
    });

    let producer_handle = thread::spawn(move || {
        if let Some(id) = network_core { core_affinity::set_for_current(id); }

        let mut journal = SequencerJournal::new("market_events.journal", 250 * 1024 * 1024);
        let mut test_data = vec![OrderMessage::default(); total_messages];
        let burst_size = 64; 

        for i in 0..total_messages {
            let is_bid = i % 2 == 0;
            let price = 50_000 + (i % 10) as u64; 
            let ticker_id = ((i / burst_size) % num_tickers) as u16;
            let user_id = (i % 1000) as u16; 
            let local_order_id = (i / num_tickers) as u64; 
            
            let mut order_type = TYPE_LIMIT;
            let mut quantity = 10;
            let mut display_quantity = 10; 
            
            if i % 7 == 0 { order_type = TYPE_IOC; }
            if i % 13 == 0 { order_type = TYPE_FOK; quantity = 50_000; display_quantity = 50_000; }
            if i % 19 == 0 { quantity = 10_000; display_quantity = 100; }
            
            if i > 1000 {
                if i % 23 == 0 { order_type = TYPE_CANCEL; }
                if i % 29 == 0 { order_type = TYPE_MODIFY; quantity = 5; } 
            }

            test_data[i] = OrderMessage {
                ticker_id, user_id, is_bid, order_type, price, 
                order_id: local_order_id, quantity, display_quantity, stop_signal: false,
            };
        }

        let mut pushed_total = 0;
        let mut journaled_total = 0;
        
        while pushed_total < total_messages {
            let to_push = (total_messages - pushed_total).min(16);
            let chunk_end = pushed_total + to_push;
            let batch = &test_data[pushed_total..chunk_end];
            
            while journaled_total < chunk_end {
                journal.append(&test_data[journaled_total]);
                journaled_total += 1;
            }

            let pushed_now = ingress_queue.push_batch(batch);
            pushed_total += pushed_now;
            
            if pushed_now == 0 { std::hint::spin_loop(); }
        }

        journal.flush();
        println!("✅ SUCCESS: All trades safely journaled and flushed to disk.");
    });

    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
}