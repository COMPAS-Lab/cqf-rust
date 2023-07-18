use std::{error::Error, usize, ops::BitAndAssign};
use bitintr::{Pdep, Tzcnt, Popcnt};
use bitvec::{prelude as bv, view::BitView, slice::BitSlice, field::BitField};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Clone, Copy)]
struct Block {
    offset: u16,
    occupieds: u64,
    runends: u64,
    remainders: [u64; 64]
}

#[derive(Default)]
pub struct RSQF {
    nslots: u64,
    nblocks: u64,
    quotient_bits: u64,
    remainder_bits: u64,
    blocks: Vec<Block>,
    iter_position: usize
}

impl RSQF {
    pub fn build(nslots: u64, key_bits: u64, value_bits: u64) -> Self {
        assert_eq!(nslots.popcnt(), 1, "nslots must be a power of 2!");
        let nblocks = nslots / 64;
        let mut blockvec: Vec<Block> = Vec::with_capacity(nblocks.try_into().unwrap());
        for _ in 0..nblocks {
            blockvec.push(Block {
                offset: 0,
                occupieds: 0,
                runends: 0,
                remainders: [0; 64]
            });
        }
        RSQF { 
            nslots: nslots,
            nblocks: nblocks,
            quotient_bits: key_bits, 
            remainder_bits: value_bits, 
            blocks: blockvec,
            ..Default::default()
        }
    }

    fn find_first_empty_slot(&self, mut from: usize) -> usize {
        loop {
            let t = self.offset_lower_bound(from);
            if t == 0 {
                break;
            }
            from += t as usize;
            // println!("from {}, t {}", from, t);
        }
        return from;
    }

    fn insert_bit_in_block(bits: &mut BitSlice<u64, bv::Lsb0>, from: usize, to: usize, bit: bool) -> bool {
        //println!("bits -- from {}, to {}, len {}", from, to, bits.len());
        //println!("before: {}", bits);
        if to == bits.len() {
            bits[from..to].rotate_right(1);
        } else {
            bits[from..=to].rotate_right(1);
        }
        let pushed_val = bits[from];
        
        bits.set(from, bit);
        //println!("after: {}, pushed_val: {}", bits, pushed_val);
        return pushed_val;
    }

    fn insert_remainders_in_block(remainders: &mut [u64], from: usize, to: usize, val: u64) -> u64 {
        //println!("remainders -- from {}, to {}, len {}", from, to, remainders.len());
        //println!("before: {:?}", remainders);
        if to == remainders.len() {
            remainders[from..to].rotate_right(1);
        } else {
            remainders[from..=to].rotate_right(1);
        }
        let pushed_val = remainders[from];
        
        remainders[from] = val;
        //println!("after: {:?}, pushed_val: {}", remainders, pushed_val);
        return pushed_val;
    }

    fn shift_remainders(&mut self, insert_index: usize, empty_slot_index: usize) {
        if insert_index != empty_slot_index {
            let mut insert_block_idx = insert_index / 64;
            let insert_block_offset = insert_index % 64;
            let empty_block_idx = empty_slot_index / 64;
            let empty_block_offset = empty_slot_index % 64;
            if empty_block_idx == insert_block_idx {
                Self::insert_remainders_in_block(&mut self.get_block_mut(insert_block_idx).remainders, insert_block_offset, empty_block_offset, 0);
            } else {
                let mut pushed_val = Self::insert_remainders_in_block(&mut self.get_block_mut(insert_block_idx).remainders, insert_block_offset, 64, 0);
                insert_block_idx += 1;

                while empty_block_idx != insert_block_idx {
                    pushed_val = Self::insert_remainders_in_block(&mut self.get_block_mut(insert_block_idx).remainders, 0, 64, pushed_val);
                    insert_block_idx += 1;
                }

                if empty_block_offset != 0 {
                    Self::insert_remainders_in_block(&mut self.get_block_mut(insert_block_idx).remainders, 0, empty_block_offset, pushed_val);
                } else {
                    self.get_block_mut(insert_block_idx).remainders[0] = pushed_val;
                }
            }
        }
    }

    fn shift_runends(&mut self, insert_index: usize, empty_slot_index: usize) {
        if insert_index != empty_slot_index {
            // println!("insert slot {}, empty {}", insert_index, empty_slot_index);
            let mut insert_block_idx = insert_index / 64;
            let insert_block_offset = insert_index % 64;
            let empty_block_idx = empty_slot_index / 64;
            let empty_block_offset = empty_slot_index % 64;
            if empty_block_idx == insert_block_idx {
                Self::insert_bit_in_block(self.get_block_mut(insert_block_idx).runends.view_bits_mut::<bv::Lsb0>(), insert_block_offset, empty_block_offset, false);
            } else {
                //println!("shifting runends on block {}", insert_block_idx);
                let mut pushed_val = Self::insert_bit_in_block(self.get_block_mut(insert_block_idx).runends.view_bits_mut::<bv::Lsb0>(), insert_block_offset, 64, false);
                insert_block_idx += 1;

                while empty_block_idx != insert_block_idx {
                    //println!("shifting runends on block {}", insert_block_idx);
                    pushed_val = Self::insert_bit_in_block(self.get_block_mut(insert_block_idx).runends.view_bits_mut::<bv::Lsb0>(), 0, 64, pushed_val);
                    insert_block_idx += 1;
                }

                //println!("shifting runends on block {}", insert_block_idx);
                if empty_block_offset != 0 {
                    Self::insert_bit_in_block(self.get_block_mut(insert_block_idx).runends.view_bits_mut::<bv::Lsb0>(), 0, empty_block_offset, pushed_val);
                } else {
                    self.get_block_mut(insert_block_idx).runends.view_bits_mut::<bv::Lsb0>().set(0, pushed_val);
                }
            }
        }
    }

    fn offset_lower_bound(&self, index: usize) -> u64 {
        let block_idx = index / 64;
        let slot = index as u64 % 64;
        //println!("calling offset_lower_bound of index {} (block {} slot {})", index, block_idx, slot);
        self.get_block(block_idx).offset_lower_bound(slot)
    }

    pub fn insert(&mut self, item: &str) -> Result<(), Box<dyn Error>> {
        self.insert1(item)
    }

    fn insert1(&mut self, item: &str) -> Result<(), Box<dyn Error>> {
        let (quotient, remainder) = self.calc_qr(item);
        let quotient_block_offset: usize = quotient % 64;
        let block_idx = quotient / 64;
        if self.is_empty(quotient) {
            self.set_slot(quotient, remainder);
            self.set_runend(quotient, true);
            self.set_occupied(quotient, true);
            // println!("insert finished into empty slot {}, new val is {}", quotient, self.get_slot(quotient));
        } else {
            // println!("slot {} is not empty", quotient);
            let mut operation = 0;
            let runend_index = self.run_end(quotient);
            let mut insert_index = runend_index + 1;
            let mut new_value = remainder;
            let mut runstart_index = if quotient == 0 { 0 } else { self.run_end(quotient - 1) + 1 };

            if self.get_block_mut(block_idx).is_occupied(quotient_block_offset) {
                // println!("slot {} is occupied", quotient);
                let mut current_remainder = self.get_slot(runstart_index);
                let mut zero_terminator = runstart_index;

                if current_remainder == 0 {
                    let mut t = runstart_index + 1;
                    while t < runend_index && self.get_slot(t) != 0 {
                        t += 1;
                    }
                    if t < runend_index && self.get_slot(t+1) == 0 {
                        zero_terminator = t + 1;
                    } else if runstart_index < runend_index && self.get_slot(runstart_index + 1) == 0 {
                        zero_terminator = runstart_index + 1;
                    }
                    if remainder != 0 {
                        runstart_index = zero_terminator + 1;
                        current_remainder = self.get_slot(runstart_index);
                    }
                }

                while current_remainder < remainder && runstart_index <= runend_index {
                    if runstart_index < runend_index && self.get_slot(runstart_index + 1) < current_remainder {
                        runstart_index += 2;
                        while runstart_index < runend_index && self.get_slot(runstart_index) != current_remainder {
                            runstart_index += 1;
                        }
                        runstart_index += 1;
                    } else {
                        runstart_index += 1;
                    }

                    current_remainder = self.get_slot(runstart_index);
                }

                if runstart_index > runend_index {
                    operation = 1;
                    insert_index = runstart_index;
                    new_value = remainder;
                } else if current_remainder != remainder {
                    operation = 2;
                    insert_index = runstart_index;
                    new_value = remainder;
                } else if runstart_index == runend_index || (remainder > 0 && self.get_slot(runstart_index + 1) > remainder) || (remainder == 0 && zero_terminator == runstart_index) {
                    operation = 2;
                    insert_index = runstart_index;
                    new_value = remainder;
                } else if (remainder > 0 && self.get_slot(runstart_index + 1) == remainder) || (remainder == 0 && zero_terminator == runstart_index + 1) {
                    operation = 2;
                    insert_index = runstart_index + 1;
                    new_value = 0;
                } else if remainder == 0 && zero_terminator == runstart_index + 2 {
                    operation = 2;
                    insert_index = runstart_index + 1;
                    new_value = 1;
                } else {
                    insert_index = runstart_index + 1;
                    while self.get_slot(insert_index + 1) != remainder {
                        insert_index += 1;
                    }
                    let (mut carry, mut digit);
                    loop {
                        carry = 0;
                        digit = self.get_slot(insert_index);
                        if digit == 0 {
                            digit += 1;
                            if digit == current_remainder {
                                digit += 1;
                            }
                        }

                        digit = (digit + 1) & bitmask(self.remainder_bits);

                        if digit == 0 {
                            digit += 1;
                            carry = 1;
                        }

                        if digit == current_remainder {
                            digit = (digit + 1) & bitmask(self.remainder_bits);
                        }
                        if digit == 0 {
                            digit += 1;
                            carry = 1;
                        }

                        self.set_slot(insert_index, digit);
                        insert_index -= 1;
                        if !(insert_index > runstart_index && carry != 0) {
                            break;
                        }
                    }

                    if insert_index == runstart_index && (carry > 0 || (current_remainder != 0 && digit >= current_remainder)) {
                        operation = 2;
                        insert_index = runstart_index + 1;
                        if carry == 0 {
                            new_value = 0;
                        } else {
                            new_value = 2;
                            if current_remainder > 0 {
                                assert!(new_value < current_remainder);
                            }
                        }
                    } else {
                        operation = -1;
                        println!("huh");
                    }
                }
            }

            if operation >= 0 {
                // println!("starting operations");
                let empty_slot_index = self.find_first_empty_slot(runend_index + 1);
                self.shift_remainders(insert_index, empty_slot_index);
                self.set_slot(insert_index, new_value);
                // println!("inserted element {} at slot {}", new_value, insert_index);
                self.shift_runends(insert_index, empty_slot_index);
                match operation {
                    0 => {
                        // self.get_block_mut(insert_index / 64).runends |= 1 << ((insert_index % 64) % 64);
                        self.set_runend(insert_index, true);
                    },
                    1 => {
                        // self.get_block_mut((insert_index - 1) / 64).runends &= !(1 << (((insert_index - 1) % 64) % 64));
                        // self.get_block_mut(insert_index / 64).runends |= 1 << ((insert_index % 64) % 64);
                        self.set_runend(insert_index - 1, false);
                        self.set_runend(insert_index, true);
                    },
                    2 => {
                        // self.get_block_mut(insert_index / 64).runends &= !(1 << ((insert_index % 64) % 64));
                        self.set_runend(insert_index, false);
                    },
                    other => panic!("Invalid operation {other}")
                }

                /*
                if (empty_slot_index % 64 == 0) {
                    println!("our empty slot is at the front of block {}!", empty_slot_index / 64);
                }
                if (empty_slot_index / 64) - (quotient/64) != 0 {
                    println!("need to increment offsets on {} blocks", (empty_slot_index / 64) - (quotient/64));
                }
                */
                for i in (((quotient / 64) + 1)..).take_while(|i: &usize| i <= &(empty_slot_index / 64)) {
                    self.get_block_mut(i).offset += 1;
                    //println!("incremented offset on block {}", i);
                }
            }
            // self.get_block_mut(quotient / 64).occupieds |= 1 << (quotient % 64);
            self.set_occupied(quotient, true);
            // println!("operation {} finished", operation);
            // println!("insert index was {} (offset was {})", insert_index, insert_index % 64);
        }
        /*
        println!("insert complete for quotient {} remainder {}", quotient, remainder);
        println!("occupied: {:#066b}", self.get_block(quotient / 64).occupieds);
        println!("runends: {:#066b}", self.get_block(quotient / 64).runends);
        */
        Ok(())
    }

    fn insert_more(&mut self, item: &str, count: usize) -> Result<(), Box<dyn Error>> {
        let (quotient, remainder) = self.calc_qr(item);
        let quotient_block_offset = quotient % 64;
        let runend_index = self.run_end(quotient);

        if self.might_be_empty(quotient) && runend_index == quotient {
            self.set_runend(quotient, true);
            self.set_slot(quotient, remainder);
            self.set_occupied(quotient, true);
            if count > 1 {
                self.insert_more(item, count - 1);
            }
        } else {
            let runstart_index = if quotient == 0 { 0 } else { self.run_end(quotient - 1) + 1 };
            
        }

        Ok(())
    }

    pub fn query(&self, item: &str) -> usize {
        let (quotient, remainder) = self.calc_qr(item);
        //println!("querying slot {} for remainder {}", quotient, remainder);
        if !self.is_occupied(quotient) {
            //println!("slot {} not occupied", quotient);
            return 0;
        } else {
            //println!("slot occupied, bits are {:#066b} and slot is {}", self.get_block(quotient / 64).occupieds, quotient % 64);
        }

        let mut runstart_index = if quotient == 0 { 0 } else { self.run_end(quotient - 1) + 1 };
        if runstart_index < quotient {
            runstart_index = quotient;
        }
        // println!("runstart index is {} for quotient {}", runstart_index, quotient);

        let mut current_end: usize;
        let mut current_remainder: u64 = 0;
        let mut current_count: usize = 0;
        loop {
            current_end = self.decode_counter(runstart_index, &mut current_remainder, &mut current_count);
            // current_remainder >>= self.remainder_bits;
            // println!("current remainder {}, remainder {}", current_remainder, remainder);
            if current_remainder == remainder {
                return current_count;
            }
            runstart_index = current_end + 1;
            if self.is_runend(current_end) { break; }
        }

        // println!("end case, returning 0, current_count was {}", current_count);
        return 0;
    }

    fn encode_counter(&self, remainder: u64, mut counter: u64, slots: &mut [u64; 67]) -> usize {
        let mut digit = remainder;
        let mut base = (1 << 32) - 1;
        
        if counter == 0 {
            return 0;
        }
        
        slots[0] = remainder;
        
        if counter == 1 {
            return 1;
        }
        
        if counter == 2 {
            slots[1] = remainder;
            return 2;
        }
        
        if counter == 3 && remainder == 0 {
            slots[1] = remainder;
            slots[2] = remainder;
            return 3;
        }
        
        if counter == 3 && remainder > 0 {
            slots[1] = 0;
            slots[2] = remainder;
            return 3;
        }
        
        if remainder == 0 {
            slots[1] = remainder;
        } else {
            base -= 1;
        }
        
        if remainder != 0 {
            counter -= 3;
        } else {
            counter -= 4;
        }
        
        let mut idx = 2;
        loop {
            digit = counter as u64 % base;
            digit += 1;
            if remainder != 0 && digit >= remainder {
                digit += 1;
            }
            slots[idx] = digit;
            counter /= base;
            idx += 1;
            if counter == 0 { break; }
        }
        
        if remainder != 0 && digit >= remainder {
            slots[idx] = 0;
        }
        
        slots[idx + 1] = remainder;
        
        return idx + 1;
    }

    fn decode_counter(&self, index: usize, remainder: &mut u64, count: &mut usize) -> usize {
        //println!("decode counter on index {} (block {} slot {})", index, index / 64, index % 64);
        let (base, rem, mut cnt, mut digit, mut end): (usize, u64, usize, u64, usize);

        rem = self.get_slot(index);
        *remainder = rem;

        if self.is_runend(index) {
            *count = 1;
            return index;
        }

        digit = self.get_slot(index + 1);

        if self.is_runend(index + 1) || (rem > 0 && digit >= rem) {
            *count = if digit == rem { 2 } else { 1 };
            return index + (if digit == rem { 1 } else { 0 });
        }

        if rem > 0 && digit == 0 && self.get_slot(index + 2) == rem {
            //println!("case 1");
            *count = 3;
            return index + 2;
        }

        if rem == 0 && digit == 0 {
            if self.get_slot(index + 2) == 0 {
                //println!("case 2");
                *count = 3;
                return index + 2;
            } else {
                *count = 2;
                return index + 1;
            }
        }

        cnt = 0;
        base = (1 << self.remainder_bits) - (if rem != 0 { 2 } else { 1 });

        end = index + 1;
        while digit != rem && !self.is_runend(end) {
            if digit > rem {
                digit -= 1;
            }
            if digit != 0 && rem != 0 {
                digit -= 1;
            }
            cnt = (cnt * base) + digit as usize;
            end += 1;
            digit = self.get_slot(end);
            // println!("cnt {}, end {}, digit {}, rem {}", cnt, end, digit, rem);
        }

        if rem != 0 {
            //println!("case 3");
            *count = cnt + 3;
            return end;
        }

        if self.is_runend(end) || self.get_slot(end + 1) != 0 {
            *count = 1;
            return index;
        }

        *count = cnt + 4;
        return end + 1;
    }

    fn calc_qr(&self, item: &str) -> (usize, u64) {
        let hash = xxh3_64(item.as_bytes());
        let quotient = (hash >> self.remainder_bits) & ((1 << self.quotient_bits) - 1);
        let remainder = hash & ((1 << self.remainder_bits) - 1);
        (quotient as usize, remainder)
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

    fn is_empty(&self, index: usize) -> bool {
        let block_idx = index / 64;
        let slot = index % 64;
        //println!("calling is_empty on {} (block {} slot {})", index, block_idx, slot);
        self.get_block(block_idx).is_empty(slot)
    }

    fn might_be_empty(&self, index: usize) -> bool {
        let block_idx = index / 64;
        let slot = index % 64;
        !self.get_block(block_idx).is_occupied(slot) && !self.get_block(block_idx).is_runend(slot)
    }

    fn get_offset(&self, index: usize) -> u16 {
        let block_idx = index / 64;
        self.get_block(block_idx).offset
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

    pub fn print_blocks(&self) {
        for (n, i) in (0..self.blocks.len()).enumerate() {
            let block = self.blocks[i];
            println!("block {}", n);
            println!("slot\t\t\tremainder\t\t\toccupied\t\t\trunend");
            for (k, (remainder, (o, r))) in std::iter::zip(block.remainders, std::iter::zip(block.occupieds.view_bits::<bv::Lsb0>(), block.runends.view_bits::<bv::Lsb0>())).enumerate() {
                println!("{} {} {} {}", k, remainder, o, r);
            }
        }
    }

    fn run_end(&self, quotient: usize) -> usize {
        let block_idx: usize = quotient / 64;
        let intrablock_offset: usize = quotient % 64;
        let block_offset: usize = self.get_block(block_idx).offset.into();
        let intrablock_rank: usize = bitrank(self.get_block(block_idx).occupieds, intrablock_offset);

        if intrablock_rank == 0 {
            if block_offset <= intrablock_offset {
                return quotient;
            } else {
                return 64 * block_idx + block_offset - 1;
            }
        }

        let mut runend_block_index: usize = block_idx + block_offset / 64;
        let mut runend_ignore_bits: usize = block_offset % 64;
        let mut runend_rank: usize = intrablock_rank - 1;
        let mut runend_block_offset: usize = bitselectv(self.get_block(runend_block_index).runends, runend_ignore_bits, runend_rank);

        if runend_block_offset == 64 {
            if block_offset == 0 && intrablock_rank == 0 {
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

#[derive(Clone, Copy, Debug)]
pub struct FilterItem {
    key: usize,
    remainder: u64,
    count: usize
}

pub struct RSQFIterator<'a> {
    qf: &'a RSQF,
    position: usize,
    run: usize,
    first: bool
}

impl<'a> IntoIterator for &'a RSQF {
    type Item = FilterItem;
    type IntoIter = RSQFIterator<'a>;

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

        RSQFIterator {
            qf: self,
            position: if position == 0 { 0 } else { self.run_end(position - 1) + 1 },
            run: position as usize,
            first: true
        }
    }
}

impl<'a> RSQFIterator<'a> {
    fn move_position(&mut self) -> bool {
        if self.position >= self.qf.nslots as usize {
            return false;
        } else {
            let (mut current_remainder, mut current_count): (u64, usize) = (0, 0);
            self.position = self.qf.decode_counter(self.position, &mut current_remainder, &mut current_count);
            if !self.qf.is_runend(self.position) {
                self.position += 1;
                if self.position >= self.qf.nslots as usize {
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
                    self.run = self.qf.nslots as usize;
                    self.position = self.qf.nslots as usize;
                    return false;
                }

                self.run = block_idx * 64 + next_run;
                self.position += 1;
                if self.position < self.run {
                    self.position = self.run;
                }

                if self.position >= self.qf.nslots as usize {
                    return false;
                }

                return true;
            }
        }
    }
}

impl<'a> Iterator for RSQFIterator<'a> {
    type Item = FilterItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.first {
            self.first = false;
            let (mut current_remainder, mut current_count): (u64, usize) = (0, 0);
            self.qf.decode_counter(self.position, &mut current_remainder, &mut current_count);
            return Some(FilterItem { key: self.position, remainder: current_remainder, count: current_count });
        }
        let can_move = self.move_position();
        if !can_move {
            return None;
        }
        let (mut current_remainder, mut current_count): (u64, usize) = (0, 0);
        self.qf.decode_counter(self.position, &mut current_remainder, &mut current_count);
        Some(FilterItem { key: self.position, remainder: current_remainder, count: current_count })
    }
}

impl Block {
    fn offset_lower_bound(&self, slot: u64) -> u64 {
        // println!("offset lower bound of slot {}", slot);
        let mut occupieds = bv::BitVec::<_, bv::Lsb0>::from_element(self.occupieds);
        let occupieds_mask = bitmask(slot+1);
        occupieds.bitand_assign(occupieds_mask.view_bits::<bv::Lsb0>());
        let offset_64: u64 = self.offset.into();
        if offset_64 <= slot {
            let mut runends = bv::BitVec::<_, bv::Lsb0>::from_element(self.runends);
            let runends_mask = bitmask(slot);
            runends.bitand_assign(runends_mask.view_bits::<bv::Lsb0>());
            runends.shift_left(offset_64 as usize);
       
            /*
            println!("offset lower bound of slot {}, occupied popcnt {}, runends popcnt {}, offset {}", slot, occupieds.load_le::<u64>().popcnt(), runends.load_le::<u64>().popcnt(), offset_64);
            println!("occupied bits:\t\t\t{}", self.occupieds.view_bits::<bv::Lsb0>());
            println!("runend bits:\t\t\t{}", self.runends.view_bits::<bv::Lsb0>());
            // println!("remainders: \t{:?}", self.remainders);
            println!("occupied mask:\t\t\t{}", occupieds_mask.view_bits::<bv::Lsb0>());
            println!("runends mask:\t\t\t{}", runends_mask.view_bits::<bv::Lsb0>());
            println!("masked occupied bits:\t{}", occupieds);
            println!("masked runend bits:\t\t{}", runends);
            */
            
            let occupieds_popcnt = occupieds.load_le::<u64>().popcnt();
            let runends_popcnt = runends.load_le::<u64>().popcnt();
            return occupieds_popcnt - runends_popcnt;
        }
        return offset_64 - slot + occupieds.load_le::<u64>().popcnt();
    }

    fn is_empty(&self, slot: usize) -> bool {
        self.offset_lower_bound(slot.try_into().unwrap()) == 0
    }

    fn is_occupied(&self, slot: usize) -> bool {
        self.occupieds.view_bits::<bv::Lsb0>()[slot]
    }

    fn set_occupied(&mut self, slot: usize, bit: bool) {
        self.occupieds.view_bits_mut::<bv::Lsb0>().set(slot, bit)
    }

    fn is_runend(&self, slot: usize) -> bool {
        self.runends.view_bits::<bv::Lsb0>()[slot]
    }

    fn set_runend(&mut self, slot: usize, bit: bool) {
        self.runends.view_bits_mut::<bv::Lsb0>().set(slot, bit)
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