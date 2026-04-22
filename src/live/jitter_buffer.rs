use crate::elem::BgpStreamElem;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

pub struct JitterBuffer<I>
where
    I: Iterator<Item = BgpStreamElem>,
{
    source: I,
    buffer: BinaryHeap<Reverse<BgpStreamElem>>,
    delay: Duration,
}

impl<I> JitterBuffer<I>
where
    I: Iterator<Item = BgpStreamElem>,
{
    pub fn new(source: I, delay: Duration) -> Self {
        Self {
            source,
            buffer: BinaryHeap::new(),
            delay,
        }
    }
}

impl<I> Iterator for JitterBuffer<I>
where
    I: Iterator<Item = BgpStreamElem>,
{
    type Item = BgpStreamElem;

    fn next(&mut self) -> Option<Self::Item> {
        // Phase 1: Pull from the source until oldest item exceeds the delay window
        while let Some(elem) = self.source.next() {
            let ts = elem.elem.timestamp;
            self.buffer.push(Reverse(elem));

            // Check if the oldest item in the heap has aged past the delay
            if let Some(Reverse(oldest)) = self.buffer.peek() {
                if ts - oldest.timestamp > self.delay.as_secs_f64() {
                    return self.buffer.pop().map(|Reverse(p)| p);
                }
            }
        }

        // Phase 2: Source is exhausted, drain the remaining buffer
        self.buffer.pop().map(|Reverse(p)| p)
    }
}

pub trait JitterBufferExt: Iterator {
    /// Wraps the iterator in a jitter buffer that reorders elements
    /// using a binary heap based on a delay threshold.
    fn jitter_buffer(self, delay: Duration) -> JitterBuffer<Self>
    where
        Self: Sized + Iterator<Item = BgpStreamElem>,
    {
        JitterBuffer::new(self, delay)
    }
}

impl<I: Iterator<Item = BgpStreamElem>> JitterBufferExt for I {}
