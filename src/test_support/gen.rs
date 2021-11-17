#![allow(dead_code)]
use crate::protocol::client::ClientConfig;
use crate::protocol::request::{Command, Request};
use crate::protocol::response::{Outcome, Response};
use crate::protocol::server::ServerConfig;
use rand::seq::SliceRandom;
use rand::Rng;
use std::net::SocketAddr;

pub struct Gen {}

impl Gen {
    pub fn u64() -> u64 {
        let mut rng = rand::thread_rng();
        rng.gen::<u64>()
    }

    pub fn usize() -> usize {
        let mut rng = rand::thread_rng();
        rng.gen::<usize>()
    }

    pub fn str() -> String {
        let strs = vec![
            "Twas brillig and the slythy toves did gyre and gimble in the wabe.".to_string(),
            "A screaming comes across the sky.".to_string(),
            "It has happened before, but there is nothing to compare it to now.".to_string(),
            "Stately, plump Buck Mulligan came from the stairhead".to_string(),
            "bearing a bowl of lather on which a mirror and a razor lay crossed.".to_string(),
        ];
        strs.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn bool() -> bool {
        vec![true, false]
            .choose(&mut rand::thread_rng())
            .unwrap()
            .clone()
    }

    pub fn socket_addr() -> SocketAddr {
        let port = port_scanner::request_open_port().unwrap();
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    pub fn request() -> Request {
        Request {
            id: Gen::u64(),
            command: Gen::command(),
        }
    }

    pub fn command() -> Command {
        let commands = vec![
            Command::Get { key: Gen::str() },
            Command::Put {
                key: Gen::str(),
                value: Gen::str(),
            },
        ];
        commands.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn response() -> Response {
        Response {
            id: Gen::u64(),
            outcome: Outcome::OfGet {
                value: Some(Gen::str()),
            },
        }
    }

    pub fn outcome() -> Outcome {
        let outcomes = vec![
            Outcome::OfGet {
                value: Some(Gen::str()),
            },
            Outcome::OfPut {
                was_modified: Gen::bool(),
            },
            Outcome::Error { msg: Gen::str() },
        ];
        outcomes.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn outcome_of(cmd: Command) -> Outcome {
        match cmd {
            Command::Get { .. } => Outcome::OfGet {
                value: Some(Gen::str()),
            },
            Command::Put { .. } => Outcome::OfPut {
                was_modified: Gen::bool(),
            },
            _ => Outcome::Error { msg: Gen::str() },
        }
    }

    pub fn server_config() -> ServerConfig {
        ServerConfig {
            address: Gen::socket_addr(),
        }
    }

    pub fn client_config() -> ClientConfig {
        ClientConfig {
            peer_addresses: vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()],
        }
    }
}
