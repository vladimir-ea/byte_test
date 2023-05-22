use std::collections::BinaryHeap;
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
    bids: BinaryHeap<OrderDetails>,
    asks: BinaryHeap<Reverse<OrderDetails>>,
}

impl OrderBook {
    /// Create a new order book using the specified connection
    pub async fn create<C: Connection>(connection: C) -> Self {
        let book = Arc::new(RwLock::new(Book {
            bids: BinaryHeap::new(),
            asks: BinaryHeap::new(),
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
            book.bids.peek().map(|order| order.price),
            book.asks.peek().map(|order| order.0.price),
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
                            Order::Bid(details) => book.bids.push(details),
                            Order::Ask(details) => book.asks.push(Reverse(details)),
                        }
                    }
                    Err(e) => {
                        // TODO: proper logging.
                        println!("Error consuming order stream: {:?}", e);

                        // Clear order book to prevent use of stale values.
                        // TODO: check if this is correct behaviour.
                        let mut order_book = book.write().await;
                        order_book.bids.clear();
                        order_book.asks.clear();
                    }
                }
            }
        }
    }
}