use std::collections::BTreeSet;
use std::cmp::Reverse;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::RwLock;

use crate::connection::Connection;
use crate::connection::Order;
use crate::connection::OrderDetails;

/// The order book
pub struct OrderBook {
    book: Arc<RwLock<Book>>,
}

struct Book {
    // TODO BTreeSet is not an optimal data structure for this, a binary heap would be better, but
    // TODO std lib binary heap does not support removal of elements, and could not immediately
    // TODO 3rd party impl of e.g. Fibonacci heap or similar.
    bids: BTreeSet<Reverse<OrderDetails>>,
    asks: BTreeSet<OrderDetails>,
}

impl OrderBook {
    /// Create a new order book using the specified connection
    pub async fn create<C: Connection>(connection: C) -> Self {
        let book = Arc::new(RwLock::new(Book {
            bids: BTreeSet::new(),
            asks: BTreeSet::new(),
        }));

        let book_clone = book.clone();
        tokio::spawn(order_book_process(book_clone, connection));
        Self {
            book
        }
    }

    /// Returns the (possibly empty) top bid and ask from the book.
    pub async fn top_bid_ask(&self) -> (Option<f32>, Option<f32>) {
        let book = self.book.read().await;
        (
            book.bids.first().map(|order| order.0.price),
            book.asks.first().map(|order| order.price),
        )
    }
}

/// Order book update process - loops indefinitely, recreating the connection stream on error.
async fn order_book_process<C: Connection>(book: Arc<RwLock<Book>>, connection: C) {
    loop {
        if let Ok(mut stream) = connection.stream().await {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(order) => {
                        let mut book = book.write().await;
                        match order {
                            Order::Bid(details) if details.quantity == 0.0 => {
                                book.bids.remove(&Reverse(details));
                            }
                            Order::Bid(details) => {
                                book.bids.insert(Reverse(details));
                            },
                            Order::Ask(details) if details.quantity == 0.0 => {
                                book.asks.remove(&details);
                            },
                            Order::Ask(details) => {
                                book.asks.insert(details);
                            },
                        }

                    }
                    Err(e) => {
                        // TODO: proper logging.
                        println!("Error consuming order stream: {:?}", e);

                        // Clear order book to prevent use of stale values.
                        // TODO: is this correct behaviour?
                        let mut order_book = book.write().await;
                        order_book.bids.clear();
                        order_book.asks.clear();
                    }
                }
            }
        }
    }
}