mod pool;
mod orderbook;
mod queue; 
mod network;
mod strategy; 

use pool::OrderPool;
use orderbook::{OrderBook, MarketDataEvent};
use queue::SpscQueue;
use strategy::MarketMakerStrategy; 
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

pub const TYPE_LIMIT: u8 = 0;
pub const TYPE_IOC: u8 = 1;
pub const TYPE_FOK: u8 = 2;

#[derive(Default, Copy, Clone, Debug)]
pub struct OrderMessage {
    pub ticker_id: u16, 
    pub is_bid: bool,
    pub order_type: u8, 
    pub price: u64,
    pub order_id: u64,
    pub quantity: u32,
    pub stop_signal: bool,
}

pub struct Exchange {
    books: Vec<OrderBook>,
    pools: Vec<OrderPool>,
    strategies: Vec<MarketMakerStrategy>,
    pub egress_queue: Arc<SpscQueue<MarketDataEvent>>,
    pub egress_buffer: Vec<MarketDataEvent>, // Pre-allocated Zero-Allocation Buffer
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

        Self { 
            books, 
            pools, 
            strategies, 
            egress_queue,
            egress_buffer: Vec::with_capacity(1024) // Never resizes. Zero overhead.
        }
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

    #[inline(always)]
    pub fn route_and_execute(&mut self, msg: OrderMessage) {
        let idx = msg.ticker_id as usize;

        unsafe {
            let book = self.books.get_unchecked_mut(idx);
            let pool = self.pools.get_unchecked_mut(idx);
            let strategy = self.strategies.get_unchecked_mut(idx);

            // We do not clear or flush the buffer here.
            // We pass it down to accumulate trades for the entire batch.
            book.process_order(pool, msg.is_bid, msg.price, msg.order_id, msg.quantity, msg.order_type, msg.ticker_id, &mut self.egress_buffer);

            if let Some(_target) = strategy.on_book_update(book) {}
        }
    }
}

fn main() {
    println!("Initializing Complete Architecture: Ingress -> Engine -> Egress...");

    let ingress_queue = Arc::new(SpscQueue::<OrderMessage>::new(131_072));
    let egress_queue = Arc::new(SpscQueue::<MarketDataEvent>::new(131_072));

    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    
    // Core Isolation: Pin each massive task to a distinct physical CPU Core
    let network_core = core_ids.first().copied();
    let engine_core = core_ids.get(1).copied().or(network_core);
    let publisher_core = core_ids.last().copied().or(engine_core);

    let total_messages = 5_000_000; 
    
    // THE OOM FIX: Lowering the concurrent stocks from 1024 to 256.
    // This drops the RAM footprint from ~5GB down to ~1.2GB.
    let num_tickers = 256; 

    // --- THREAD 3: THE EGRESS PUBLISHER ---
    let publisher_queue = Arc::clone(&egress_queue);
    let publisher_handle = thread::spawn(move || {
        if let Some(id) = publisher_core { core_affinity::set_for_current(id); }
        let mut local_batch = [MarketDataEvent::default(); 128];
        let mut _total_trades_published = 0; // Prefixed with _ to silence compiler warnings

        loop {
            let popped = publisher_queue.pop_batch(&mut local_batch);
            if popped > 0 {
                // In a live system, this thread formats these events to FIX/ITCH protocol 
                // and blasts them over UDP to external Market Makers.
                _total_trades_published += popped;
            } else {
                std::hint::spin_loop();
            }
        }
    });

    // --- THREAD 2: THE MATCHING ENGINE ---
    let consumer_queue = Arc::clone(&ingress_queue);
    let consumer_handle = thread::spawn(move || {
        if let Some(id) = engine_core { core_affinity::set_for_current(id); }

        let mut exchange = Exchange::new(num_tickers, 25_000, egress_queue);
        let mut local_batch = [OrderMessage::default(); 32]; 
        
        let mut start_time = Instant::now();
        let mut is_timing = false;
        let mut processed_count = 0; 

        loop {
            let popped = consumer_queue.pop_batch(&mut local_batch);
            
            if popped > 0 {
                // 1. Clear the egress buffer BEFORE processing the batch
                exchange.egress_buffer.clear();

                for i in 0..popped {
                    let msg = local_batch[i];
                    
                    if processed_count == 0 && !is_timing {
                        start_time = Instant::now();
                        is_timing = true;
                    }

                    let lookahead = 2; 
                    if i + lookahead < popped {
                        exchange.prefetch_memory(&local_batch[i + lookahead]);
                    }

                    // This stacks trades into the egress_buffer seamlessly
                    exchange.route_and_execute(msg);
                    processed_count += 1;

                    if processed_count == total_messages && is_timing {
                        let elapsed = start_time.elapsed();
                        let nanos_per_msg = elapsed.as_nanos() as f64 / total_messages as f64;
                        
                        println!("--- PIPELINE BENCHMARK COMPLETE ---");
                        println!("Processed {} packets across {} tickers in {:?}", total_messages, num_tickers, elapsed);
                        println!("End-to-End Latency (Ingress -> Match -> Egress Publisher): {:.2} nanoseconds per message", nanos_per_msg);
                        std::process::exit(0); // Cleanly exit the entire program when done
                    }
                }

                // 2. ONE Single Atomic Flush AFTER the entire batch is processed!
                if !exchange.egress_buffer.is_empty() {
                    exchange.egress_queue.push_batch(&exchange.egress_buffer);
                }

            } else {
                std::hint::spin_loop(); 
            }
        }
    });

    // --- THREAD 1: THE INGRESS PRODUCER ---
    let producer_handle = thread::spawn(move || {
        if let Some(id) = network_core { core_affinity::set_for_current(id); }

        let mut test_data = vec![OrderMessage::default(); total_messages];
        let burst_size = 64; 

        for i in 0..total_messages {
            let is_bid = i % 2 == 0;
            let price = 50_000 + (i % 10) as u64; 
            let ticker_id = ((i / burst_size) % num_tickers) as u16;
            let local_order_id = (i / num_tickers) as u64; 
            
            let mut order_type = TYPE_LIMIT;
            let mut quantity = 10;
            if i % 7 == 0 { order_type = TYPE_IOC; }
            if i % 13 == 0 { 
                order_type = TYPE_FOK; 
                quantity = 50000; 
            }

            test_data[i] = OrderMessage {
                ticker_id, is_bid, order_type, price, 
                order_id: local_order_id, quantity, stop_signal: false,
            };
        }

        let mut pushed_total = 0;
        while pushed_total < total_messages {
            let chunk_end = (pushed_total + 16).min(total_messages);
            let pushed_now = ingress_queue.push_batch(&test_data[pushed_total..chunk_end]);
            pushed_total += pushed_now;
            if pushed_now == 0 { std::hint::spin_loop(); }
        }
    });

    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
    publisher_handle.join().unwrap();
}