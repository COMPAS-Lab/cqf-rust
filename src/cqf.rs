use std::{usize, path::PathBuf, fs::File};
use std::io::{BufWriter, BufReader};
use bitintr::{Pdep, Tzcnt, Popcnt};
use xxhash_rust::xxh3::xxh3_64;
use itertools::Itertools;
use bincode::{Encode, Decode};
use anyhow::Result;

#[derive(Encode, Decode, Clone, Copy)]
struct Block {
    offset: u16,
    occupieds: u64,
    runends: u64,
    counts: u64,
    remainders: [u64; 64]
}

#[derive(Encode, Decode, PartialEq, Clone, Copy, Debug, Default)]
pub enum HashMode {
    Invertible,
    #[default]
    Fast
}

#[derive(Encode, Decode, Default)]
pub struct CQF {
    lognslots: u64,
    nslots: u64,
    xnslots: u64,
    nblocks: u64,
    noccupied_slots: u64,
    quotient_bits: u64,
    remainder_bits: u64,
    hash_mode: HashMode,
    blocks: Vec<Block>
}

impl CQF {
    pub fn build(lognslots: u64, key_bits: u64, hash_mode: HashMode) -> Self {
        let nslots = 1 << lognslots;
        assert_eq!(nslots.popcnt(), 1, "nslots must be a power of 2!");
        let xnslots: u64 = (nslots as f32 + 10.0*((nslots as f32).sqrt())) as u64;
        let nblocks = (xnslots + 63) / 64;
        let mut blockvec: Vec<Block> = Vec::with_capacity(nblocks.try_into().unwrap());
        for _ in 0..nblocks {
            blockvec.push(Block {
                offset: 0,
                occupieds: 0,
                runends: 0,
                counts: 0,
                remainders: [0; 64]
            });
        }
        CQF { 
            lognslots: lognslots,
            nslots: nslots,
            xnslots: xnslots,
            nblocks: nblocks,
            quotient_bits: key_bits, 
            remainder_bits: 64 - key_bits, 
            hash_mode,
            blocks: blockvec,
            ..Default::default()
        }
    }

    pub fn from(qf1: Self, qf2: Self, lognslots: u64, key_bits: u64) -> Self {
        let nslots = 1 << lognslots;
        assert_eq!(nslots.popcnt(), 1, "nslots must be a power of 2!");
        assert_eq!(qf1.hash_mode, qf2.hash_mode, "CQFs must have the same hash mode!");
        let xnslots: u64 = (nslots as f32 + 10.0*((nslots as f32).sqrt())) as u64;
        let nblocks = (xnslots + 63) / 64;
        let mut blockvec: Vec<Block> = Vec::with_capacity(nblocks.try_into().unwrap());
        for _ in 0..nblocks {
            blockvec.push(Block {
                offset: 0,
                occupieds: 0,
                runends: 0,
                counts: 0,
                remainders: [0; 64]
            });
        }
        let mut new = Self { 
            lognslots: lognslots,
            nslots: nslots,
            xnslots: xnslots,
            nblocks: nblocks,
            quotient_bits: key_bits, 
            remainder_bits: 64 - key_bits, 
            hash_mode: qf1.hash_mode,
            blocks: blockvec,
            ..Default::default()
        };
        let merged = qf1.into_iter().merge(qf2.into_iter());
        for item in merged {
            new.insert_by_hash(item.hash, item.count).expect("couldn't insert into new CQF!");
        }
        new
    }

    pub fn from_multi(qfs: Vec<&Self>, lognslots: u64, key_bits: u64) -> Self {
        let nslots = 1 << lognslots;
        assert_eq!(nslots.popcnt(), 1, "nslots must be a power of 2!");
        // should check each qf's hashmode here...
        let xnslots: u64 = (nslots as f32 + 10.0*((nslots as f32).sqrt())) as u64;
        let nblocks = (xnslots + 63) / 64;
        let mut blockvec: Vec<Block> = Vec::with_capacity(nblocks.try_into().unwrap());
        for _ in 0..nblocks {
            blockvec.push(Block {
                offset: 0,
                occupieds: 0,
                runends: 0,
                counts: 0,
                remainders: [0; 64]
            });
        }
        let mut new = Self { 
            lognslots: lognslots,
            nslots: nslots,
            xnslots: xnslots,
            nblocks: nblocks,
            quotient_bits: key_bits, 
            remainder_bits: 64 - key_bits, 
            hash_mode: qfs[0].hash_mode,
            blocks: blockvec,
            ..Default::default()
        };
        let merged = qfs.into_iter().kmerge();
        for item in merged {
            new.insert_by_hash(item.hash, item.count).expect("couldn't insert into new CQF!");
        }
        new
    }

    pub fn resize(&mut self, lognslots: u64, key_bits: u64) {
        let nslots = 1 << lognslots;
        assert_eq!(nslots.popcnt(), 1, "nslots must be a power of 2!");
        let xnslots: u64 = (nslots as f32 + 10.0*((nslots as f32).sqrt())) as u64;
        let nblocks = (xnslots + 63) / 64;
        let mut blockvec: Vec<Block> = Vec::with_capacity(nblocks.try_into().unwrap());
        for _ in 0..nblocks {
            blockvec.push(Block {
                offset: 0,
                occupieds: 0,
                runends: 0,
                counts: 0,
                remainders: [0; 64]
            });
        }
        let mut new = Self { 
            lognslots: lognslots,
            nslots: nslots,
            xnslots: xnslots,
            nblocks: nblocks,
            quotient_bits: key_bits, 
            remainder_bits: 64 - key_bits, 
            hash_mode: self.hash_mode,
            blocks: blockvec,
            ..Default::default()
        };
        for item in self.into_iter() {
            new.insert_by_hash(item.hash, item.count).expect("couldn't insert into new CQF!");
        }
        *self = new;
    }

    pub fn serialize(&self, path: PathBuf) -> Result<()> {
        let mut file = BufWriter::new(File::create(path)?);
        bincode::encode_into_std_write(self, &mut file, bincode::config::standard())?;
        Ok(())
    }

    pub fn deserialize(path: PathBuf) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let deserialized: CQF = bincode::decode_from_std_read(&mut file, bincode::config::standard())?;
        Ok(deserialized)
    }

    fn find_first_empty_slot(&self, mut from: usize) -> usize {
        loop {
            let t = self.offset_lower_bound(from);
            if t == 0 {
                break;
            }
            from += t as usize;
        }
        return from;
    }

    fn shift_remainders(&mut self, insert_index: usize, empty_slot_index: usize, distance: usize) {
        for i in (insert_index..=empty_slot_index).rev() {
            self.set_slot(i + distance, self.get_slot(i));
        }
    }

    fn shift_runends(&mut self, insert_index: usize, empty_slot_index: usize, distance: usize) {
        for i in (insert_index..=empty_slot_index).rev() {
            self.set_runend(i + distance, self.is_runend(i));
        }
    }

    fn shift_counts(&mut self, insert_index: usize, empty_slot_index: usize, distance: usize) {
        for i in (insert_index..=empty_slot_index).rev() {
            self.set_count(i + distance, self.is_count(i));
        }
    }

    fn offset_lower_bound(&self, index: usize) -> u64 {
        let block_idx = index / 64;
        let slot = index as u64 % 64;
        self.get_block(block_idx).offset_lower_bound(slot)
    }

    pub fn get_load_factor(&self) -> f32 {
        self.noccupied_slots as f32 / self.xnslots as f32
    }

    pub fn check_and_resize(&mut self) {
        if self.get_load_factor() >= 0.95 {
            println!("CQF is filling up, resizing...");
            self.resize(self.lognslots + 1, self.quotient_bits + 1);
            println!("resize successful!");
        }
    }

    pub fn insert(&mut self, item: u64, count: u64) -> Result<()> {
        self.check_and_resize();

        let hash = self.calc_hash(item);
        self.insert_by_hash(hash, count)
    }

    pub fn insert_by_hash(&mut self, hash: u64, count: u64) -> Result<()> {
        self.check_and_resize();

        let (quotient, remainder) = self.calc_qr(hash);
        let runend_index = self.run_end(quotient);

        if self.might_be_empty(quotient) && runend_index == quotient {
            self.set_runend(quotient, true);
            self.set_slot(quotient, remainder);
            self.set_occupied(quotient, true);
            self.noccupied_slots += 1;
            if count > 1 {
                self.insert_by_hash(hash, count - 1)?;
            }
        } else {
            let mut runstart_index = if quotient == 0 { 0 } else { self.run_end(quotient - 1) + 1 };
            if !self.is_occupied(quotient) {
                self.insert_and_shift(0, quotient, remainder, count, runstart_index, 0);
            } else {
                let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
                let mut current_end: usize;
                current_end = self.decode_counter(runstart_index, &mut current_remainder, &mut current_count);
                while current_remainder < remainder && !self.is_runend(current_end) {
                    runstart_index = current_end + 1;
                    current_end = self.decode_counter(runstart_index, &mut current_remainder, &mut current_count)
                }

                if current_remainder < remainder {
                    self.insert_and_shift(1, quotient, remainder, count, current_end + 1, 0);
                } else if current_remainder == remainder {
                    self.insert_and_shift(if self.is_runend(current_end) { 1 } else { 2 }, quotient, remainder, current_count + count, runstart_index, current_end - runstart_index + 1);
                } else {
                    self.insert_and_shift(2, quotient, remainder, count, runstart_index, 0);
                }
            }
            self.set_occupied(quotient, true);
        }

        Ok(())
    }

    fn insert_and_shift(&mut self, operation: u64, quotient: usize, remainder: u64, count: u64, insert_index: usize, noverwrites: usize) {
        let ninserts = if count == 1 { 1 } else { 2 } - noverwrites;
        if ninserts > 0 {
            match ninserts {
                1 => {
                    let empty = self.find_first_empty_slot(insert_index);
                    self.shift_remainders(insert_index, empty - 1, 1);
                    self.shift_runends(insert_index, empty - 1, 1);
                    self.shift_counts(insert_index, empty - 1, 1);
                    let mut npreceding_empties = 0;
                    for i in (((quotient / 64) + 1)..).take_while(|i: &usize| *i <= empty / 64) {
                        while npreceding_empties < ninserts && empty / 64 < i {
                            npreceding_empties += 1;
                        }

                        self.get_block_mut(i).offset += (ninserts - npreceding_empties) as u16;
                    }
                },
                2 => {
                    let first = self.find_first_empty_slot(insert_index);
                    let second = self.find_first_empty_slot(first + 1);
                    self.shift_remainders(first + 1, second - 1, 1);
                    self.shift_runends(first + 1, second - 1, 1);
                    self.shift_counts(first + 1, second - 1, 1);
                    self.shift_remainders(insert_index, first - 1, 2);
                    self.shift_runends(insert_index, first - 1, 2);
                    self.shift_counts(insert_index, first - 1, 2);

                    let empties = [first, second];
                    let mut npreceding_empties = 0;
                    for i in (((quotient / 64) + 1)..).take_while(|i: &usize| *i <= empties[ninserts - 1] / 64) {
                        while npreceding_empties < ninserts && empties[npreceding_empties] / 64 < i {
                            npreceding_empties += 1;
                        }

                        self.get_block_mut(i).offset += (ninserts - npreceding_empties) as u16;
                    }
                },
                _ => panic!("unexpected number of inserts!")
            }

            match operation {
                0 => {
                    if count == 1 {
                        self.set_runend(insert_index, true);
                    } else {
                        self.set_runend(insert_index, false);
                        self.set_runend(insert_index + 1, true);
                    }
                },
                1 => {
                    if noverwrites == 0 {
                        self.set_runend(insert_index - 1, false);
                    }
                    if count == 1 {
                        self.set_runend(insert_index, true);
                    } else {
                        self.set_runend(insert_index, false);
                        self.set_runend(insert_index + 1, true);
                    }
                },
                2 => {
                    if count == 1 {
                        self.set_runend(insert_index, false);
                    } else {
                        self.set_runend(insert_index, false);
                        self.set_runend(insert_index + 1, false);
                    }
                },
                _ => panic!("invalid operation!"),
            }
        }
        
        self.set_slot(insert_index, remainder);
        if count != 1 {
            // if the count isn't one, put a count in the next slot
            self.set_count(insert_index + 1, true);
            self.set_slot(insert_index + 1, count);
        }
        self.noccupied_slots += ninserts as u64;
    }

    pub fn query(&self, item: u64) -> u64 {
        self.query_by_hash(self.calc_hash(item))
    }

    pub fn query_by_hash(&self, hash: u64) -> u64 {
        let (quotient, remainder) = self.calc_qr(hash);
        if !self.is_occupied(quotient) {
            return 0;
        }
        let mut runstart_index = if quotient == 0 { 0 } else { self.run_end(quotient - 1) + 1 };
        if runstart_index < quotient {
            runstart_index = quotient;
        }
        let mut current_end: usize;
        let mut current_remainder: u64 = 0;
        let mut current_count: u64 = 0;
        loop {
            current_end = self.decode_counter(runstart_index, &mut current_remainder, &mut current_count);
            if current_remainder == remainder {
                return current_count;
            }
            if self.is_runend(current_end) { break; }
            runstart_index = current_end + 1;
        }
        return 0;
    }

    fn decode_counter(&self, index: usize, remainder: &mut u64, count: &mut u64) -> usize {
        *remainder = self.get_slot(index);

        // if it's a runend or the next thing is not a count, there's only one
        if self.is_runend(index) || !self.is_count(index + 1) {
            *count = 1;
            return index; 
        } else { // otherwise, whatever is in the next slot is the count
            *count = self.get_slot(index + 1);
            return index + 1;
        }
    }

    fn calc_hash(&self, item: u64) -> u64 {
        match self.hash_mode {
            HashMode::Invertible => {
                let mut key = item;
                key = (!key).wrapping_add(key << 21); // key = (key << 21) - key - 1;
                key = key ^ (key >> 24);
                key = (key.wrapping_add(key << 3)).wrapping_add(key << 8); // key * 265
                key = key ^ (key >> 14);
                key = (key.wrapping_add(key << 2)).wrapping_add(key << 4); // key * 21
                key = key ^ (key >> 28);
                key = key.wrapping_add(key << 31);
                key
            },
            HashMode::Fast => xxh3_64(&item.to_le_bytes()),
        }
    }

    pub fn invert_hash(&self, item: u64) -> Option<u64> {
        match self.hash_mode {
            HashMode::Invertible => {
                let mut tmp: u64;
                let mut key = item;

                // Invert key = key + (key << 31)
                tmp = key.wrapping_sub(key<<31);
                key = key.wrapping_sub(tmp<<31);

                // Invert key = key ^ (key >> 28)
                tmp = key^key>>28;
                key = key^tmp>>28;

                // Invert key *= 21
                key = key.wrapping_mul(14933078535860113213);

                // Invert key = key ^ (key >> 14)
                tmp = key^key>>14;
                tmp = key^tmp>>14;
                tmp = key^tmp>>14;
                key = key^tmp>>14;

                // Invert key *= 265
                key = key.wrapping_mul(15244667743933553977);

                // Invert key = key ^ (key >> 24)
                tmp = key^key>>24;
                key = key^tmp>>24;

                // Invert key = (~key) + (key << 21)
                tmp = !key;
                tmp = !(key.wrapping_sub(tmp<<21));
                tmp = !(key.wrapping_sub(tmp<<21));
                key = !(key.wrapping_sub(tmp<<21));

                Some(key)
            },
            HashMode::Fast => None,
        }
    }

    fn calc_qr(&self, hash: u64) -> (usize, u64) {
        let quotient = (hash >> self.remainder_bits) & ((1 << self.quotient_bits) - 1);
        let remainder = hash & ((1 << self.remainder_bits) - 1);
        (quotient as usize, remainder)
    }

    pub fn build_hash(&self, quotient: usize, remainder: u64) -> u64 {
        ((quotient as u64) << self.remainder_bits) | remainder
    }

    fn is_occupied(&self, index: usize) -> bool {
        let block_idx = index / 64;
        let slot = index % 64;
        self.get_block(block_idx).is_occupied(slot)
    }

    fn set_occupied(&mut self, index: usize, val: bool) {
        let block_idx = index / 64;
        let slot = index % 64;
        self.get_block_mut(block_idx).set_occupied(slot, val)
    }

    fn is_runend(&self, index: usize) -> bool {
        let block_idx = index / 64;
        let slot = index % 64;
        self.get_block(block_idx).is_runend(slot)
    }

    fn set_runend(&mut self, index: usize, val: bool) {
        let block_idx = index / 64;
        let slot = index % 64;
        self.get_block_mut(block_idx).set_runend(slot, val)
    }

    fn is_count(&self, index: usize) -> bool {
        let block_idx = index / 64;
        let slot = index % 64;
        self.get_block(block_idx).is_count(slot)
    }

    fn set_count(&mut self, index: usize, val: bool) {
        let block_idx = index / 64;
        let slot = index % 64;
        self.get_block_mut(block_idx).set_count(slot, val)
    }

    fn get_slot(&self, index: usize) -> u64 {
        let block_idx = index / 64;
        let slot = index % 64;
        self.get_block(block_idx).get_slot(slot)
    }

    fn set_slot(&mut self, index: usize, val: u64) {
        let block_idx = index / 64;
        let slot = index % 64;
        self.get_block_mut(block_idx).set_slot(slot, val)
    }

    fn might_be_empty(&self, index: usize) -> bool {
        let block_idx = index / 64;
        let slot = index % 64;
        !self.get_block(block_idx).is_occupied(slot) && !self.get_block(block_idx).is_runend(slot)
    }

    fn get_block(&self, block_idx: usize) -> &Block {
        match self.blocks.get(block_idx) {
            Some(block) => block,
            None => panic!("Tried getting block at idx {}, we only have {} blocks", block_idx, self.blocks.len())
        }
    }

    fn get_block_mut(&mut self, block_idx: usize) -> &mut Block {
        let nblocks = self.blocks.len();
        match self.blocks.get_mut(block_idx) {
            Some(block) => block,
            None => panic!("Tried getting block at idx {}, we only have {} blocks", block_idx, nblocks)
        }
    }

    fn run_end(&self, quotient: usize) -> usize {
        let block_idx: usize = quotient / 64;
        let intrablock_offset: usize = quotient % 64;
        let blocks_offset: usize = self.get_block(block_idx).offset.into();
        let intrablock_rank: usize = bitrank(self.get_block(block_idx).occupieds, intrablock_offset);

        if intrablock_rank == 0 {
            if blocks_offset <= intrablock_offset {
                return quotient;
            } else {
                return 64 * block_idx + blocks_offset - 1;
            }
        }

        let mut runend_block_index: usize = block_idx + blocks_offset / 64;
        let mut runend_ignore_bits: usize = blocks_offset % 64;
        let mut runend_rank: usize = intrablock_rank - 1;
        let mut runend_block_offset: usize = bitselectv(self.get_block(runend_block_index).runends, runend_ignore_bits, runend_rank);

        if runend_block_offset == 64 {
            if blocks_offset == 0 && intrablock_rank == 0 {
                return quotient;
            } else {
                loop {
                    runend_rank -= popcntv(self.get_block(runend_block_index).runends, runend_ignore_bits);
                    runend_block_index += 1;
                    runend_ignore_bits = 0;
                    runend_block_offset = bitselectv(self.get_block(runend_block_index).runends, runend_ignore_bits, runend_rank);
                    if runend_block_offset != 64 { break; }
                }
            }
        }

        let runend_index = 64 * runend_block_index + runend_block_offset;
        if runend_index < quotient {
            quotient
        } else {
            runend_index
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Hash)]
pub struct FilterItem {
    pub hash: u64,
    pub item: Option<u64>,
    pub count: u64
}

pub struct CQFIterator<'a> {
    qf: &'a CQF,
    position: usize,
    run: usize,
    first: bool
}

impl<'a> IntoIterator for &'a CQF {
    type Item = FilterItem;
    type IntoIter = CQFIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut position = 0;
        if !self.is_occupied(0) {
            let mut block_index: usize = 0;
            let mut idx = bitselect(self.get_block(0).occupieds, 0);
            if idx == 64 {
                while idx == 64 && block_index < (self.nblocks - 1) as usize {
                    block_index += 1;
                    idx = bitselect(self.get_block(block_index).occupieds, 0);
                }
            }
            position = block_index * 64 + idx;
        }

        CQFIterator {
            qf: self,
            position: if position == 0 { 0 } else { self.run_end(position - 1) + 1 },
            run: position as usize,
            first: true
        }
    }
}

impl<'a> CQFIterator<'a> {
    fn move_position(&mut self) -> bool {
        if self.position >= self.qf.xnslots as usize {
            return false;
        } else {
            let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
            self.position = self.qf.decode_counter(self.position, &mut current_remainder, &mut current_count);
            if !self.qf.is_runend(self.position) {
                self.position += 1;
                if self.position >= self.qf.xnslots as usize {
                    return false;
                }
                return true;
            } else {
                let mut block_idx = self.run / 64;
                let mut rank = bitrank(self.qf.get_block(block_idx).occupieds, self.run % 64);
                let mut next_run = bitselect(self.qf.get_block(block_idx).occupieds, rank);

                if next_run == 64 {
                    rank = 0;
                    while next_run == 64 && block_idx < (self.qf.nblocks - 1) as usize {
                        block_idx += 1;
                        next_run = bitselect(self.qf.get_block(block_idx).occupieds, rank);
                    }
                }

                if block_idx == self.qf.nblocks as usize {
                    self.run = self.qf.xnslots as usize;
                    self.position = self.qf.xnslots as usize;
                    return false;
                }

                self.run = block_idx * 64 + next_run;
                self.position += 1;
                if self.position < self.run {
                    self.position = self.run;
                }

                if self.position >= self.qf.xnslots as usize {
                    return false;
                }

                return true;
            }
        }
    }
}

impl<'a> Iterator for CQFIterator<'a> {
    type Item = FilterItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.first {
            self.first = false;
            let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
            self.qf.decode_counter(self.position, &mut current_remainder, &mut current_count);
            let hash = self.qf.build_hash(self.run, current_remainder);
            return Some(FilterItem { hash, item: self.qf.invert_hash(hash), count: current_count });
        }
        let can_move = self.move_position();
        if !can_move {
            return None;
        }
        let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
        self.qf.decode_counter(self.position, &mut current_remainder, &mut current_count);
        let hash = self.qf.build_hash(self.run, current_remainder);
        Some(FilterItem { hash, item: self.qf.invert_hash(hash), count: current_count })
    }
}

impl Block {
    fn offset_lower_bound(&self, slot: u64) -> u64 {
        let occupieds = self.occupieds & bitmask(slot+1);
        let offset_64: u64 = self.offset.into();
        if offset_64 <= slot {
            let runends = (self.runends & bitmask(slot)) >> offset_64;
            return occupieds.popcnt() - runends.popcnt();
        }
        return offset_64 - slot + occupieds.popcnt();
    }

    fn is_occupied(&self, slot: usize) -> bool {
        ((self.occupieds >> slot) & 1) != 0
    }

    fn set_occupied(&mut self, slot: usize, bit: bool) {
        if bit {
            self.occupieds |= 1 << slot;
        } else {
            self.occupieds &= !(1 << slot);
        }
    }

    fn is_runend(&self, slot: usize) -> bool {
        ((self.runends >> slot) & 1) != 0
    }

    fn set_runend(&mut self, slot: usize, bit: bool) {
        if bit {
            self.runends |= 1 << slot;
        } else {
            self.runends &= !(1 << slot);
        }
    }
    
    fn is_count(&self, slot: usize) -> bool {
        ((self.counts >> slot) & 1) != 0
    }

    fn set_count(&mut self, slot: usize, bit: bool) {
        if bit {
            self.counts |= 1 << slot;
        } else {
            self.counts &= !(1 << slot);
        }
    }

    fn set_slot(&mut self, slot: usize, value: u64) {
        self.remainders[slot] = value;
    }

    fn get_slot(&self, slot: usize) -> u64 {
        self.remainders[slot]
    }
}

fn bitrank(val: u64, pos: usize) -> usize {
    if pos == 63 {
        (val & u64::MAX).popcnt() as usize
    } else {
        (val & ((2 << pos) - 1)).popcnt() as usize
    }
}

fn popcntv(val: u64, ignore: usize) -> usize {
    if ignore % 64 != 0 {
        (val & !(bitmask(ignore as u64 % 64))).popcnt() as usize
    } else {
        val.popcnt() as usize
    }
}

fn bitselect(val: u64, rank: usize) -> usize {
    (1 << rank as u64).pdep(val).tzcnt() as usize
}

fn bitselectv(val: u64, ignore: usize, rank: usize) -> usize {
    bitselect(val & !(bitmask(ignore as u64 % 64)), rank)
}

fn bitmask(nbits: u64) -> u64 {
    if nbits == 64 { u64::MAX } else { (1 << nbits) - 1 }
}