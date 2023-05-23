use std::collections::VecDeque;
use std::convert::TryFrom;
use std::convert::TryInto;

use async_trait::async_trait;
use futures::{future, Stream, StreamExt, TryFutureExt, TryStreamExt};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::Message;

use super::Connection;
use super::ConnectionStream;
use super::Error;
use super::Order;
use super::OrderDetails;

/// A connection to Binance for the specified symbol
pub struct BinanceConnection {
    symbol: String,
}

impl BinanceConnection {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_owned(),
        }
    }
}

#[async_trait]
impl Connection for BinanceConnection {
    async fn stream(&self) -> Result<ConnectionStream, Error> {
        let snapshot_url = format!(
            "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
            self.symbol
        );
        let stream_url = format!(
            "wss://stream.binance.com:9443/ws/{}@depth",
            self.symbol.to_lowercase()
        );

        // Start the stream
        let mut delta_stream = stream(&stream_url).await?;
        let mut delta_buffer = VecDeque::new();

        let snapshot = loop {
            tokio::select! {
                delta_result = delta_stream.next() => {
                    match delta_result {
                        Some(Ok(delta)) => delta_buffer.push_back(delta),
                        Some(Err(e)) => {
                            break Err(Error::from(e));
                        }
                        None => {
                            break Err(Error::Stream(TungsteniteError::ConnectionClosed));
                        }
                    }
                }
                snapshot_result = snapshot(&snapshot_url) => {
                    break snapshot_result;
                }
            }
        }?;

        // Drop any buffered deltas that predate the snapshot.
        let last_updated = snapshot.last_update_id;
        while let Some(delta) = delta_buffer.front() {
            if delta.last_update <= last_updated {
                delta_buffer.pop_front();
            } else {
                break;
            }
        }

        // If there is a buffered delta, check the update times.
        if let Some(delta) = delta_buffer.front() {
            if delta.first_update > last_updated + 1 || delta.last_update < last_updated + 1 {
                return Err(Error::UnexpectedItem(format!(
                    "Bad update bounds: {}, {:?}",
                    last_updated + 1,
                    delta
                )));
            }
        }

        // Convert to order stream
        let snapshot_orders: Vec<Order> = snapshot.try_into()?;
        let snapshot_stream =
            futures::stream::iter(snapshot_orders.into_iter().map(|order| Ok(order)));

        // Convert buffered deltas to stream and chain with live stream
        let buffer_stream =
            futures::stream::iter(delta_buffer.into_iter().map(|d| Ok::<_, Error>(d)));
        let delta_stream = buffer_stream.chain(delta_stream);
        let deltas = delta_stream
            .map(|result| match result {
                Ok(delta) => {
                    let result: Result<Vec<Order>, Error> = delta.try_into();
                    match result {
                        Ok(orders) => {
                            let stream = futures::stream::iter(
                                orders.into_iter().map(|o| Ok::<_, Error>(o)),
                            );
                            Ok(stream)
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            })
            .try_flatten();

        Ok(Box::pin(snapshot_stream.chain(deltas)))
    }
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    /// last update time in snapshot
    #[serde(rename(deserialize = "lastUpdateId"))]
    last_update_id: u64,
    /// Snapshot bids (price, quantity)
    bids: Vec<(String, String)>,
    /// Snapshot asks (price, quantity)
    asks: Vec<(String, String)>,
}

impl TryFrom<Snapshot> for Vec<Order> {
    type Error = Error;

    fn try_from(value: Snapshot) -> Result<Self, Self::Error> {
        let mut asks = to_asks(value.asks)?;
        let bids = to_bids(value.bids)?;
        asks.extend(bids);
        Ok(asks)
    }
}

fn to_asks(raw: Vec<(String, String)>) -> Result<Vec<Order>, Error> {
    raw.into_iter()
        .map(|a| Ok(Order::Ask(a.try_into()?)))
        .collect::<Result<Vec<_>, _>>()
}

fn to_bids(raw: Vec<(String, String)>) -> Result<Vec<Order>, Error> {
    raw.into_iter()
        .map(|a| Ok(Order::Bid(a.try_into()?)))
        .collect::<Result<Vec<_>, _>>()
}

impl TryFrom<(String, String)> for OrderDetails {
    type Error = Error;

    fn try_from((p, q): (String, String)) -> Result<Self, Self::Error> {
        Ok(OrderDetails {
            price: p.parse::<f32>()?,
            quantity: q.parse::<f32>()?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct Delta {
    /// event type (always "depthUpdate")
    #[serde(skip)]
    e: String,
    /// event timestamp
    #[serde(rename(deserialize = "E"))]
    event_time: u64,
    /// symbol (always requested symbol)
    #[serde(skip)]
    s: String,
    /// First update id
    #[serde(rename(deserialize = "U"))]
    first_update: u64,
    /// Last update id
    #[serde(rename(deserialize = "u"))]
    last_update: u64,
    /// bids
    b: Vec<(String, String)>,
    /// asks
    a: Vec<(String, String)>,
}

impl TryFrom<Delta> for Vec<Order> {
    type Error = Error;

    fn try_from(value: Delta) -> Result<Self, Self::Error> {
        let mut asks = to_asks(value.a)?;
        let bids = to_bids(value.b)?;
        asks.extend(bids);
        Ok(asks)
    }
}

async fn snapshot(url: &str) -> Result<Snapshot, Error> {
    Ok(reqwest::get(url).await?.json::<Snapshot>().await?)
}

async fn stream(url: &str) -> Result<impl Stream<Item = Result<Delta, Error>>, Error> {
    let (stream, _) = tokio_tungstenite::connect_async(url).await?;

    Ok(stream.map(|result| match result {
        Ok(msg) => match msg {
            Message::Text(s) => serde_json::from_str::<Delta>(&s).map_err(Error::from),
            other => Err(Error::UnexpectedItem(format!("{:?}", other))),
        },
        Err(e) => Err(Error::Stream(e)),
    }))
}
