use std::{
    collections::{HashMap, VecDeque},
    fmt,
    io::{self},
    sync::{atomic::AtomicBool, Mutex, RwLock},
};

use crossbeam_channel::Sender;

use crate::{Message, Outbound};

pub(crate) struct SharedState {
    pub(crate) id: String,
    pub(crate) shutting_down: AtomicBool,
    pub(crate) last_error: RwLock<io::Result<()>>,
    pub(crate) subs: RwLock<HashMap<usize, (String, Option<String>, Sender<Message>)>>,
    pub(crate) pongs: Mutex<VecDeque<Sender<bool>>>,
    pub(crate) writer: Mutex<Outbound>,
    pub(crate) disconnect_callback: RwLock<Option<Box<dyn Fn() + Send + Sync + 'static>>>,
    pub(crate) reconnect_callback: RwLock<Option<Box<dyn Fn() + Send + Sync + 'static>>>,
}

impl fmt::Debug for SharedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let dc_cb = self.disconnect_callback.read().unwrap();
        let rc_cb = self.reconnect_callback.read().unwrap();

        f.debug_map()
            .entry(&"id", &self.id)
            .entry(&"shutting_down", &self.shutting_down)
            .entry(&"last_error", &self.last_error)
            .entry(&"subs", &self.subs)
            .entry(&"pongs", &self.pongs)
            .entry(&"writer", &self.writer)
            .entry(
                &"disconnect_callback",
                if dc_cb.is_some() { &"set" } else { &"unset" },
            )
            .entry(
                &"reconnect_callback",
                if rc_cb.is_some() { &"set" } else { &"unset" },
            )
            .finish()
    }
}
