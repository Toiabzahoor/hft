use crate::{OrderMessage, SpscQueue};
use std::net::UdpSocket;
use std::sync::Arc;
use std::io;

#[inline(always)]
pub fn parse_message(raw_bytes: &[u8]) -> OrderMessage {
    let msg_type = raw_bytes[0];
    
    let price = u64::from_le_bytes(raw_bytes[1..9].try_into().unwrap());
    let order_id = u64::from_le_bytes(raw_bytes[9..17].try_into().unwrap());
    let quantity = u32::from_le_bytes(raw_bytes[17..21].try_into().unwrap());

    OrderMessage {
        is_bid: msg_type == b'B',
        price,
        order_id,
        quantity,
        stop_signal: false,
    }
}

/// Binds to a port, pins the thread to a core, and spins forever waiting for packets.
pub fn start_udp_listener(port: u16, queue: Arc<SpscQueue<OrderMessage>>, core_id: Option<core_affinity::CoreId>) {
    // 1. Core Pinning
    if let Some(id) = core_id {
        if core_affinity::set_for_current(id) {
            println!("Network Thread pinned strictly to Core {}", id.id);
        } else {
            println!("Warning: Failed to pin Network Thread to Core {}. (Codespace hypervisor might restrict this)", id.id);
        }
    }

    // 2. Socket Setup
    let address = format!("0.0.0.0:{}", port);
    let socket = UdpSocket::bind(&address).expect("Failed to bind UDP socket");
    
    // THE MAGIC LINE: This prevents the OS from ever putting our thread to sleep.
    socket.set_nonblocking(true).expect("Failed to set non-blocking");

    println!("Listening for raw UDP packets on {}...", address);

    let mut buffer = [0u8; 1024]; // L1 cache friendly buffer
    let mut batch = [OrderMessage::default(); 16];
    let mut batch_count = 0;

    // 3. The Busy Polling Loop
    loop {
        match socket.recv_from(&mut buffer) {
            Ok((size, _src)) => {
                // We assume perfect 21-byte packets for this HFT setup
                if size == 21 {
                    batch[batch_count] = parse_message(&buffer[..21]);
                    batch_count += 1;

                    // If our local batch is full, flush it to the lock-free queue
                    if batch_count == 16 {
                        let mut pushed = 0;
                        while pushed < 16 {
                            pushed += queue.push_batch(&batch[pushed..16]);
                            if pushed < 16 { std::hint::spin_loop(); }
                        }
                        batch_count = 0;
                    }
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // NO DATA YET. 
                // Do NOT sleep. Do NOT yield. 
                // Just emit a hardware pause to save power and loop again immediately.
                
                // If we have a partial batch sitting here, we should push it so it doesn't get stale
                if batch_count > 0 {
                    let mut pushed = 0;
                    while pushed < batch_count {
                        pushed += queue.push_batch(&batch[pushed..batch_count]);
                        if pushed < batch_count { std::hint::spin_loop(); }
                    }
                    batch_count = 0;
                }
                
                std::hint::spin_loop();
            }
            Err(e) => {
                eprintln!("Socket error: {}", e);
                break;
            }
        }
    }
}