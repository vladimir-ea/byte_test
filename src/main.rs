use connection::BinanceConnection;
use order_book::OrderBook;

mod connection;
mod order_book;

#[tokio::main]
async fn main() {
    let binance_connection = BinanceConnection::new("BTCUSDT");
    let order_book = OrderBook::create(binance_connection).await;

    loop {
        let (bid, ask) = order_book.top_bid_ask().await;
        println!("Bid: {}, Ask: {}", bid.unwrap_or(f32::NAN), ask.unwrap_or(f32::NAN));
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
