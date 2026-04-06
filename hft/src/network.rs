#![allow(dead_code)]
use crate::{OrderMessage, SpscQueue};
use std::net::UdpSocket;
use std::sync::Arc;
use std::io;

#[inline(always)]
pub fn parse_message(raw_bytes: &[u8]) -> OrderMessage {
    let msg_type = raw_bytes[0];
    let order_type = raw_bytes[1]; 
    let ticker_id = u16::from_le_bytes(raw_bytes[2..4].try_into().unwrap());
    let user_id = u16::from_le_bytes(raw_bytes[4..6].try_into().unwrap()); 
    
    let price = u64::from_le_bytes(raw_bytes[6..14].try_into().unwrap());
    let order_id = u64::from_le_bytes(raw_bytes[14..22].try_into().unwrap());
    let quantity = u32::from_le_bytes(raw_bytes[22..26].try_into().unwrap());

    OrderMessage {
        ticker_id,
        user_id,
        is_bid: msg_type == b'B',
        order_type,
        price,
        order_id,
        quantity,
        display_quantity: quantity, // FIX: Default to fully visible for raw network packets
        stop_signal: false,
    }
}

pub fn start_udp_listener(port: u16, queue: Arc<SpscQueue<OrderMessage>>, core_id: Option<core_affinity::CoreId>) {
    if let Some(id) = core_id {
        if core_affinity::set_for_current(id) {
            println!("Network Thread pinned strictly to Core {}", id.id);
        }
    }

    let address = format!("0.0.0.0:{}", port);
    let socket = UdpSocket::bind(&address).expect("Failed to bind UDP socket");
    socket.set_nonblocking(true).expect("Failed to set non-blocking");

    let mut buffer = [0u8; 1024]; 
    let mut batch = [OrderMessage::default(); 16];
    let mut batch_count = 0;

    loop {
        match socket.recv_from(&mut buffer) {
            Ok((size, _src)) => {
                if size == 26 {
                    batch[batch_count] = parse_message(&buffer[..26]);
                    batch_count += 1;

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