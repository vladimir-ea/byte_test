#![feature(test)]
extern crate test;

use std::collections::BinaryHeap;

use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use test::Bencher;
use futures::StreamExt;
use futures::Stream;

// Binary heap of string.
#[bench]
fn no_parse_to_number(b: &mut Bencher) {
    let mut rng = ChaCha8Rng::seed_from_u64(23);
    let mut heap = BinaryHeap::new();
    b.iter(|| {
         for _ in 0..100_000 {
             let value = rng.gen::<u64>().to_string();
             heap.push(value);
         }
         heap.clear();
    });
}

// Binary heap of u64 from parsed strings
#[bench]
fn parse_to_number(b: &mut Bencher) {
    let mut rng = ChaCha8Rng::seed_from_u64(23);
    let mut heap = BinaryHeap::new();
    b.iter(|| {
        for _ in 0..100_000 {
            let value = rng.gen::<u64>().to_string();
            heap.push(value.parse::<u64>().unwrap());
        }
        heap.clear();
    });
}