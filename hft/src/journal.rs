#![allow(dead_code)]
use crate::OrderMessage;
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;
use std::mem;

pub struct SequencerJournal {
    mmap: MmapMut,
    offset: usize,
}

impl SequencerJournal {
    pub fn new<P: AsRef<Path>>(path: P, size: usize) -> Self {
        println!("Allocating {} MB Memory-Mapped Journal at {:?}...", size / 1024 / 1024, path.as_ref());
        
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) 
            .open(path)
            .expect("Failed to open journal file");
        
        file.set_len(size as u64).expect("Failed to set journal size");
        
        let mmap = unsafe { MmapMut::map_mut(&file).expect("Failed to map memory") };
        
        Self { mmap, offset: 0 }
    }

    #[inline(always)]
    pub fn append(&mut self, msg: &OrderMessage) {
        let size = mem::size_of::<OrderMessage>(); 
        
        if self.offset + size <= self.mmap.len() {
            unsafe {
                let src = msg as *const _ as *const u8;
                let dst = self.mmap.as_mut_ptr().add(self.offset);
                std::ptr::copy_nonoverlapping(src, dst, size);
            }
            self.offset += size;
        } else {
            panic!("CRITICAL: Journal capacity exhausted!");
        }
    }

    // Force the OS to write the RAM buffer to the physical hard drive
    pub fn flush(&self) {
        self.mmap.flush().expect("Failed to flush journal to disk");
    }
}