use std::{
    collections::{HashMap, VecDeque},
    io::{self},
    sync::{Mutex, RwLock},
};

use crossbeam_channel::Sender;

use crate::{Message, Outbound};

#[derive(Debug)]
pub(crate) struct SharedState {
    pub(crate) last_error: RwLock<io::Result<()>>,
    pub(crate) subs: RwLock<HashMap<usize, Sender<Message>>>,
    pub(crate) pongs: Mutex<VecDeque<Sender<bool>>>,
    pub(crate) writer: Mutex<Outbound>,
}
