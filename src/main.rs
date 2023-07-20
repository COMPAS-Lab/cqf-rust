use std::error::Error;
use std::collections::HashSet;

use cqf_rust::*;
use itertools::Itertools;
use rand::{distributions::Alphanumeric, Rng};
use indicatif::{ProgressIterator, ProgressStyle};

fn main() -> Result<(), Box<dyn Error>> {
    //insert_qf();
    //iterate_qf();
    merge_qf()?;
    Ok(())
}

fn insert_qf() {
    let mut qf = CQF::build(1 << 22, 20, 32);

    let mut strings: Vec<String> = Vec::new();
    let n_strings: usize = 10_000_000;

    let progress_bar_style = ProgressStyle::with_template("elapsed: {elapsed_precise} | total: {duration_precise} | speed: {per_sec} {wide_bar} {pos:>7}/{len:7}").unwrap();
    println!("generating strings...");
    for _ in (0..n_strings).progress_with_style(progress_bar_style.clone()) {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(12)
            .map(char::from)
            .collect();
        strings.push(s);
    }

    println!("inserting into CQF...");
    for i in (0..n_strings/2).progress_with_style(progress_bar_style.clone()) {
        let _ = qf.insert(strings[i].as_str(), 1);
    }
    println!("checking for false negatives...");
    for i in (0..n_strings/2).progress_with_style(progress_bar_style.clone()) {
        assert!(qf.query(strings[i].as_str()) > 0, "false negative!");
    }
    println!("checking for false positives...");
    let mut present: u32 = 0;
    for i in (n_strings/2..n_strings).progress_with_style(progress_bar_style.clone()) {
        if qf.query(strings[i].as_str()) > 0 {
            present += 1;
        }
    }
    println!("Number of false positives: {} (FPR: {})", present, present as f32 / 5000.0);
    assert!(present as f32 / 500000.0 < (2 as f32).powf(-16.0));
}

fn iterate_qf() {
    let mut qf = CQF::build(1 << 25, 24, 32);

    let mut strings: Vec<String> = Vec::new();
    let n_strings: usize = 10_000_000;
    let count = 3;

    let progress_bar_style = ProgressStyle::with_template("elapsed: {elapsed_precise} | total: {duration_precise} | speed: {per_sec} {wide_bar} {pos:>7}/{len:7}").unwrap();
    println!("generating strings...");
    for _ in (0..n_strings).progress_with_style(progress_bar_style.clone()) {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(12)
            .map(char::from)
            .collect();
        strings.push(s);
    }

    println!("inserting into CQF...");
    for i in (0..n_strings/2).progress_with_style(progress_bar_style.clone()) {
        let _ = qf.insert(strings[i].as_str(), count);
    }
    //qf.print_blocks();
    let mut present: u32 = 0;
    println!("checking for false negatives...");
    for i in (0..n_strings/2).progress_with_style(progress_bar_style.clone()) {
        assert!(qf.query(strings[i].as_str()) > 0, "false negative!");
        if qf.query(strings[i].as_str()) > count {
            present += 1;
        }
    }
    println!("checking for false positives...");
    for i in (n_strings/2..n_strings).progress_with_style(progress_bar_style.clone()) {
        if qf.query(strings[i].as_str()) > 0 {
            present += 1;
        }
    }
    println!("Number of false positives: {} (FPR: {})", present, present as f32 / 5000.0);
    assert!(present as f32 / 500000.0 < (2 as f32).powf(-16.0));
    println!("Iterating over items in CQF...");
    let mut counter = 0;
    for item in qf.into_iter().progress_count(n_strings as u64 / 2) {
        counter += 1;
    }
    assert_eq!(counter, n_strings / 2);
    println!("All {} strings were enumerated by the CQF!", n_strings / 2);
}

fn merge_qf() -> Result<(), Box<dyn Error>> {
    let mut qf1 = CQF::build(1 << 25, 24, 40);
    let mut qf2 = CQF::build(1 << 25, 24, 40);
    let mut qf3 = CQF::build(1 << 28, 27, 37);

    let mut strings: Vec<String> = Vec::new();
    let n_strings: usize = 10_000_000;
    let count = 3;

    let progress_bar_style = ProgressStyle::with_template("elapsed: {elapsed_precise} | total: {duration_precise} | speed: {per_sec} {wide_bar} {pos:>7}/{len:7}").unwrap();
    println!("generating strings...");
    for _ in (0..n_strings).progress_with_style(progress_bar_style.clone()) {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(12)
            .map(char::from)
            .collect();
        strings.push(s);
    }

    println!("inserting into CQF 1...");
    for i in (0..n_strings/2).progress_with_style(progress_bar_style.clone()) {
        qf1.insert(strings[i].as_str(), count)?;
    }
    println!("inserting into CQF 2...");
    for i in (n_strings/2..n_strings).progress_with_style(progress_bar_style.clone()) {
        qf2.insert(strings[i].as_str(), count)?;
    }

    let mut items = HashSet::with_capacity(n_strings);
    println!("Iterating over items in CQFs 1 and 2 to collect items...");
    for item in qf1.into_iter().progress_count(n_strings as u64 / 2) {
        items.insert(item);
    }
    for item in qf2.into_iter().progress_count(n_strings as u64 / 2) {
        items.insert(item);
    }

    println!("Merging CQFs 1 and 2 into 3...");
    //qf1.merge(&qf2, &mut qf3)?;
    let merged = qf1.into_iter().merge(qf2.into_iter());
    for item in merged.progress_count(n_strings as u64) {
        qf3.insert_by_hash(item.hash, item.count)?;
    }

    println!("Iterating over items in CQF 3 and checking items...");
    let mut merge_items = HashSet::with_capacity(n_strings);
    for item in qf3.into_iter().progress_count(n_strings as u64) {
        merge_items.insert(item);
    }
    let diffs = merge_items.symmetric_difference(&items).count();
    assert_eq!(diffs, 0);
    println!("All {} items matched!", n_strings);
    Ok(())
}