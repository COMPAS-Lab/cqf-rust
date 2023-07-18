mod rsqf;
pub use rsqf::RSQF;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{distributions::Alphanumeric, Rng};

    #[test]
    fn create_qf() {
        let _qf = RSQF::build(1 << 8, 16);
    }

    #[test]
    fn insert_qf() {
        let mut qf = RSQF::build(1 << 20, 20);

        let mut strings: Vec<String> = Vec::new();
        for _ in 0..10 {
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
            strings.push(s);
        }

        for item in &strings[0..10] {
            let _ = qf.insert(item.as_str());
        }
        // checking for false negatives
        for item in &strings[0..5] {
            assert!(qf.query(item.as_str()) > 0);
        }
        // checking FPR
        let mut present: u32 = 0;
        for item in &strings[5..] {
            if qf.query(item.as_str()) > 0 {
                present += 1;
            }
        }
        assert!(present as f32 / 500000.0 < (2 as f32).powf(-16.0));
    }
}