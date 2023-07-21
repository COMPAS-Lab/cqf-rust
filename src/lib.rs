mod cqf;
pub use cqf::CQF;

#[cfg(test)]
mod tests {
    use std::{error::Error, collections::HashSet};

    use super::*;
    use rand::{distributions::Alphanumeric, Rng};

    #[test]
    fn insert() -> Result<(), Box<dyn Error>> {
        let mut qf = CQF::build(23, 23);

        let n_strings: usize = 10_000_000;
        let mut strings: Vec<String> = Vec::with_capacity(n_strings);

        for _ in 0..n_strings {
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
            strings.push(s);
        }

        for i in 0..n_strings/2 {
            qf.insert(strings[i].as_bytes(), 3)?;
        }
        for i in 0..n_strings/2 {
            assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
        }
        let mut present: u32 = 0;
        for i in n_strings/2..n_strings {
            if qf.query(strings[i].as_bytes()) > 0 {
                present += 1;
            }
        }
        assert_eq!(present, 0);
        Ok(())
    }

    #[test]
    fn enumerate() -> Result<(), Box<dyn Error>> {
        let mut qf = CQF::build(25, 25);

        let n_strings: usize = 10_000_000;
        let count = 3;
        let mut strings: Vec<String> = Vec::with_capacity(n_strings);

        for _ in 0..n_strings {
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
            strings.push(s);
        }

        for i in 0..n_strings/2 {
            qf.insert(strings[i].as_bytes(), count)?;
        }
        
        let mut present: u32 = 0;
        for i in 0..n_strings/2 {
            assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
            if qf.query(strings[i].as_bytes()) > count {
                present += 1;
            }
        }
        for i in n_strings/2..n_strings {
            if qf.query(strings[i].as_bytes()) > 0 {
                present += 1;
            }
        }
        assert_eq!(present, 0);
        let mut counter = 0;
        for _ in qf.into_iter() {
            counter += 1;
        }
        assert_eq!(counter, n_strings / 2);
        Ok(())
    }

    #[test]
    fn merge() -> Result<(), Box<dyn Error>> {
        let mut qf1 = CQF::build(25, 25);
        let mut qf2 = CQF::build(25, 25);

        let n_strings: usize = 10_000_000;
        let count = 3;
        let mut strings: Vec<String> = Vec::with_capacity(n_strings);

        for _ in 0..n_strings {
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
            strings.push(s);
        }

        for i in 0..n_strings/2 {
            qf1.insert(strings[i].as_bytes(), count)?;
        }
        for i in n_strings/2..n_strings {
            qf2.insert(strings[i].as_bytes(), count)?;
        }

        let mut items = HashSet::with_capacity(n_strings);
        for item in qf1.into_iter() {
            items.insert(item);
        }
        for item in qf2.into_iter() {
            items.insert(item);
        }

        let qf3 = CQF::from(qf1, qf2, 28, 28);

        let mut merge_items = HashSet::with_capacity(n_strings);
        for item in qf3.into_iter() {
            merge_items.insert(item);
        }
        let diffs = merge_items.symmetric_difference(&items).count();
        assert_eq!(diffs, 0);

        for i in 0..n_strings {
            assert!(qf3.query(strings[i].as_bytes()) > 0, "false negative!");
        }
        Ok(())
    }
}