mod cqf;
pub use cqf::*;

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::PathBuf}; 

    use super::*;
    use rand::Rng;
    use anyhow::Result;

    #[test]
    fn insert() -> Result<()> {
        let mut qf = CQF::build(23, 23, HashMode::Fast);

        let n_strings: usize = 10_000_000;
        //let mut strings: Vec<String> = Vec::with_capacity(n_strings);
        let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

        let mut rng = rand::thread_rng();
        for _ in 0..n_strings {
            /*
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
                strings.push(s);
            */
            numbers.push(rng.gen())
        }

        for i in 0..n_strings/2 {
            //qf.insert(strings[i].as_bytes(), 3)?;
            qf.insert(numbers[i], 3)?;
        }
        for i in 0..n_strings/2 {
            //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
            assert!(qf.query(numbers[i]) > 0, "false negative!");
        }
        let mut present: u32 = 0;
        for i in n_strings/2..n_strings {
            /*
            if qf.query(strings[i].as_bytes()) > 0 {
                present += 1;
            }
            */
            if qf.query(numbers[i]) > 0 {
                present += 1;
            }
        }
        assert_eq!(present, 0);
        Ok(())
    }

    #[test]
    fn enumerate() -> Result<()> {
        let mut qf = CQF::build(25, 25, HashMode::Fast);

        let n_strings: usize = 10_000_000;
        let count = 3;
        //let mut strings: Vec<String> = Vec::with_capacity(n_strings);
        let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

        let mut rng = rand::thread_rng();
        for _ in 0..n_strings {
            /*
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
                strings.push(s);
            */
            numbers.push(rng.gen())
        }

        for i in 0..n_strings/2 {
            //qf.insert(strings[i].as_bytes(), count)?;
            qf.insert(numbers[i], count)?;
        }
        
        let mut present: u32 = 0;
        for i in 0..n_strings/2 {
            /*
            assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
            if qf.query(strings[i].as_bytes()) > count {
                present += 1;
            }
            */
            assert!(qf.query(numbers[i]) > 0, "false negative!");
            if qf.query(numbers[i]) > count {
                present += 1;
            }
        }
        for i in n_strings/2..n_strings {
            /*
            if qf.query(strings[i].as_bytes()) > 0 {
                present += 1;
            }
            */
            if qf.query(numbers[i]) > 0 {
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
    fn merge() -> Result<()> {
        let mut qf1 = CQF::build(25, 25, HashMode::Fast);
        let mut qf2 = CQF::build(25, 25, HashMode::Fast);

        let n_strings: usize = 10_000_000;
        let count = 3;
        //let mut strings: Vec<String> = Vec::with_capacity(n_strings);
        let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

        let mut rng = rand::thread_rng();
        for _ in 0..n_strings {
            /*
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
                strings.push(s);
            */
            numbers.push(rng.gen())
        }

        for i in 0..n_strings/2 {
            //qf1.insert(strings[i].as_bytes(), count)?;
            qf1.insert(numbers[i], count)?;
        }
        for i in n_strings/2..n_strings {
            //qf2.insert(strings[i].as_bytes(), count)?;
            qf2.insert(numbers[i], count)?;
        }

        let mut items = HashSet::with_capacity(n_strings);
        for item in qf1.into_iter() {
            items.insert(item);
        }
        for item in qf2.into_iter() {
            items.insert(item);
        }

        let qf3 = CQF::from(qf1, qf2);

        let mut merge_items = HashSet::with_capacity(n_strings);
        for item in qf3.into_iter() {
            merge_items.insert(item);
        }
        let diffs = merge_items.symmetric_difference(&items).count();
        assert_eq!(diffs, 0);

        for i in 0..n_strings {
            //assert!(qf3.query(strings[i].as_bytes()) > 0, "false negative!");
            assert!(qf3.query(numbers[i]) > 0, "false negative!");
        }
        Ok(())
    }

    #[test]
    fn serialize() -> Result<()> {
        let mut qf = CQF::build(23, 23, HashMode::Invertible);

        let n_strings: usize = 10_000_000;
        //let mut strings: Vec<String> = Vec::with_capacity(n_strings);
        let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

        let mut rng = rand::thread_rng();
        for _ in 0..n_strings {
            /*
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
                strings.push(s);
            */
            numbers.push(rng.gen())
        }

        for i in 0..n_strings/2 {
            //qf.insert(strings[i].as_bytes(), 3)?;
            qf.insert(numbers[i], 3)?;
        }
        for i in 0..n_strings/2 {
            //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
            assert!(qf.query(numbers[i]) > 0, "false negative!");
        }
        let mut present: u32 = 0;
        for i in n_strings/2..n_strings {
            /*
            if qf.query(strings[i].as_bytes()) > 0 {
                present += 1;
            }
            */
            if qf.query(numbers[i]) > 0 {
                present += 1;
            }
        }
        assert_eq!(present, 0);

        qf.serialize(PathBuf::from("/home/ari/Documents/GitHub/cqf-rust/serialize-test.cqf"))?;
        let read_qf = CQF::deserialize(PathBuf::from("/home/ari/Documents/GitHub/cqf-rust/serialize-test.cqf"))?;
        let mut items = HashSet::with_capacity(n_strings);
        for item in qf.into_iter() {
            items.insert(item);
        }

        let mut serial_items = HashSet::with_capacity(n_strings);
        for item in read_qf.into_iter() {
            serial_items.insert(item);
        }
        let diffs = serial_items.symmetric_difference(&items).count();
        assert_eq!(diffs, 0);
        Ok(())
    }

    #[test]
    fn invert() -> Result<()> {
        let mut qf = CQF::build(25, 25, HashMode::Invertible);

        let n_vals: usize = 10_000_000;
        let count = 3;
        let mut numbers: Vec<u64> = Vec::with_capacity(n_vals);

        let mut rng = rand::thread_rng();
        for _ in 0..n_vals {
            let number = rng.gen();
            numbers.push(number);
        }

        let mut number_set: HashSet<u64> = HashSet::with_capacity(n_vals);
        for i in 0..n_vals/2 {
            qf.insert(numbers[i], count)?;
            number_set.insert(numbers[i]);
        }
        
        let mut present: u32 = 0;
        for i in 0..n_vals/2 {
            assert!(qf.query(numbers[i]) > 0, "false negative!");
            if qf.query(numbers[i]) > count {
                present += 1;
            }
        }
        for i in n_vals/2..n_vals {
            if qf.query(numbers[i]) > 0 {
                present += 1;
            }
        }
        assert_eq!(present, 0);
        let mut counter = 0;
        let mut enumerated_set: HashSet<u64> = HashSet::with_capacity(n_vals);
        for item in qf.into_iter() {
            enumerated_set.insert(item.item.unwrap());
            counter += 1;
        }
        assert_eq!(counter, n_vals / 2, "we didn't get the right number of enumerated items!");
        assert!(enumerated_set == number_set, "enumerated items don't match originals!");
        Ok(())
    }
}