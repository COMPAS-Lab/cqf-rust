use cqf_rust::*;
use rand::{distributions::Alphanumeric, Rng};
use indicatif::{ProgressIterator, ProgressStyle};

fn main() {
    //insert_qf();
    iterate_qf();
}

fn insert_qf() {
    let mut qf = RSQF::build(1 << 22, 20, 32);

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
        let _ = qf.insert(strings[i].as_str());
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
    let mut qf = RSQF::build(1 << 22, 24, 32);

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
        let _ = qf.insert(strings[i].as_str());
    }
    //qf.print_blocks();
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
    println!("Iterating over items in CQF...");
    let mut counter = 0;
    for item in qf.into_iter().progress_count(n_strings as u64 / 2) {
        counter += 1;
    }
    assert_eq!(counter, n_strings / 2);
    println!("All {} strings were enumerated by the CQF!", n_strings / 2);
}