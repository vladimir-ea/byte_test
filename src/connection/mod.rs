use std::cmp::Ordering;
use std::num::ParseFloatError;
use std::pin::Pin;

use async_trait::async_trait;
use reqwest::Error as ReqwestError;
use serde_json::Error as JsonError;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use futures::Stream;

mod binance;

pub use binance::BinanceConnection;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Rest(#[from] ReqwestError),
    #[error(transparent)]
    Stream(#[from] TungsteniteError),
    #[error(transparent)]
    ParseJson(#[from] JsonError),
    #[error("Unexpected stream item {0}")]
    UnexpectedItem(String),
    #[error(transparent)]
    ParseFloat(#[from] ParseFloatError),
}

/// Order
pub enum Order {
    /// Bid order
    Bid(OrderDetails),
    /// Ask order
    Ask(OrderDetails),
}

/// Type of a connection stream
pub type ConnectionStream = Pin<Box<dyn Stream<Item = Result<Order, Error>> + Send + Sync>>;

/// Trait implemented by different connections
#[async_trait]
pub trait Connection: Send + Sync + 'static {
    async fn stream(&self) -> Result<ConnectionStream, Error>;
}

/// An order.
pub struct OrderDetails {
    pub price: f32,
    pub quantity: f32,
}

impl PartialEq for OrderDetails {
    fn eq(&self, other: &Self) -> bool {
        self.price == other.price
    }
}

impl Eq for OrderDetails {}

impl Ord for OrderDetails {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.price < other.price {
            Ordering::Less
        } else if self.price > other.price {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for OrderDetails {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}