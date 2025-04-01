use std::time::Duration;
use futures_signals::signal::{ Signal, SignalExt, SignalStream};
use futures::stream::{ StreamExt};
use rand::Rng;
use tokio::time;

const MIN_SECONDS: f64 = 1.0;
const MAX_SECONDS:f64 = 5.0;
const MIN_VALUE:f64 = 90.0;
const MAX_VALUE:f64 = 98.0;

#[tokio::main]
async fn main() {
   combine_latest_example().await;
}

async fn random_signal(){
    create_stream()
        .map(|item| { item.unwrap_or_else(|| 0.0) })
        .map(|item| {item + 10.0})
        .map(|item| {
            println!("After +10 : {}",item);
            item
        })
        .map(|item| {i64::from(item as u32)})
        .dedupe()
        .for_each(|item| {
            println!("Result :{}", item);
            async {}
        }).await
}

async fn combine_latest_example(){
    let stream =create_stream().map(|item| { item.unwrap_or_else(|| 0.0) });

    let stream2 =create_stream().map(|item| { item.unwrap_or_else(|| 0.0) });


    SignalStream::zip(stream.to_stream(), stream2.to_stream())
        .for_each(|(a, b)| async move {
            println!("A {} B {}", a, b)
        }).await;
}

fn create_stream() -> impl Signal<Item = Option<f64>> {
    let mut rng = rand::rng();
    futures_signals::signal::from_stream(async_stream::stream! {
        loop {
            let delay = rng.random_range(MIN_SECONDS..=MAX_SECONDS);
            let value = rng.random_range(MIN_VALUE..=MAX_VALUE);
            time::sleep(Duration::from_secs_f64(delay)).await;
            yield value;
        }
    })

}

