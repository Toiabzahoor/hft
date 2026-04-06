#![allow(dead_code)]
use crate::OrderMessage;

#[derive(Clone, Copy, Debug)]
pub struct UserState {
    pub max_position: u32,
    pub current_exposure: u32,
    pub active_orders_count: u32,
}

impl Default for UserState {
    fn default() -> Self {
        Self {
            max_position: 1_000_000, 
            current_exposure: 0,
            active_orders_count: 0,
        }
    }
}

pub struct RiskGateway {
    users: Vec<UserState>, 
}

impl RiskGateway {
    pub fn new(max_users: usize) -> Self {
        println!("Allocating Risk Gateway for {} users...", max_users);
        Self {
            users: vec![UserState::default(); max_users],
        }
    }

    #[inline(always)]
    pub fn check_pre_trade(&mut self, msg: &OrderMessage) -> bool {
        if (msg.user_id as usize) >= self.users.len() {
            return false; 
        }

        let user = unsafe { self.users.get_unchecked_mut(msg.user_id as usize) };

        if msg.quantity > 50_000 {
            return false; 
        }

        let new_exposure = user.current_exposure + msg.quantity;
        if new_exposure > user.max_position {
            return false; 
        }

        user.current_exposure = new_exposure;
        user.active_orders_count += 1;

        true 
    }
}