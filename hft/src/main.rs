mod pool;
mod orderbook;
mod queue; 
mod network; 

use pool::OrderPool;
use orderbook::OrderBook;
use queue::SpscQueue;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[derive(Default, Copy, Clone, Debug)]
pub struct OrderMessage {
    pub is_bid: bool,
    pub price: u64,
    pub order_id: u64,
    pub quantity: u32,
    pub stop_signal: bool,
}

fn main() {
    println!("Initializing Full Live HFT Node...");

    let queue = Arc::new(SpscQueue::<OrderMessage>::new(131_072));
    let consumer_queue = Arc::clone(&queue);

    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let engine_core = core_ids.last().copied();
    let network_core = core_ids.first().copied();

    // --- CONSUMER THREAD (The Engine) ---
    thread::spawn(move || {
        if let Some(id) = engine_core {
            core_affinity::set_for_current(id);
        }

        let capacity = 2_000_000;
        let mut order_pool = OrderPool::new(capacity);
        let mut order_book = OrderBook::new(100_000, capacity);
        let mut local_batch = [OrderMessage::default(); 32]; 
        
        // Timer variables
        let mut start_time = Instant::now();
        let mut is_timing = false;

        loop {
            let popped = consumer_queue.pop_batch(&mut local_batch);
            
            if popped > 0 {
                for i in 0..popped {
                    let msg = local_batch[i];
                    
                    // START THE CLOCK on the first packet
                    if msg.order_id == 0 && !is_timing {
                        start_time = Instant::now();
                        is_timing = true;
                        println!("First packet received! Stopwatch started...");
                    }

                    order_book.add_order(
                        &mut order_pool, 
                        msg.is_bid, 
                        msg.price, 
                        msg.order_id, 
                        msg.quantity
                    );

                    // STOP THE CLOCK on the 1,000,000th packet
                    if msg.order_id == 999_999 && is_timing {
                        let elapsed = start_time.elapsed();
                        let messages = 1_000_000;
                        let nanos_per_msg = elapsed.as_nanos() as f64 / messages as f64;
                        
                        println!("--- LIVE FLOOD TEST COMPLETE ---");
                        println!("Processed {} packets from OS network stack in {:?}", messages, elapsed);
                        println!("Average System Latency (Kernel + Rust Pipeline): {:.2} nanoseconds", nanos_per_msg);
                        
                        // Reset for the next flood
                        is_timing = false; 
                    }
                }
            } else {
                std::hint::spin_loop(); 
            }
        }
    });

    // --- PRODUCER THREAD (The Live UDP Listener) ---
    network::start_udp_listener(8080, queue, network_core);
}