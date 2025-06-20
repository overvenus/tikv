use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct TermCache {
    boundaries: VecDeque<(u64, u64)>, // (start_index, term)
    last_log_index: Option<u64>,      // highest index currently stored
    first_log_index: Option<u64>,     // lowest index currently stored
}

impl TermCache {
    pub fn new() -> Self {
        Self {
            boundaries: VecDeque::new(),
            last_log_index: None,
            first_log_index: None,
        }
    }
    /// Insert a new continuous log entry at a certain index and term.
    /// Assumes entries are always contiguous.
    pub fn insert(&mut self, index: u64, term: u64) {
        // Remove all boundaries with index >= given index
        while let Some(&(start, _)) = self.boundaries.back() {
            if start >= index {
                self.boundaries.pop_back();
            } else {
                break;
            }
        }
        // Only insert new entry if the last term is different
        if self
            .boundaries
            .back()
            .map_or(true, |&(_, last_term)| last_term != term)
        {
            self.boundaries.push_back((index, term));
        }
        // Update first and last indices
        if self.first_log_index.is_none() || index < self.first_log_index.unwrap() {
            self.first_log_index = Some(index);
        }
        self.last_log_index = Some(index);
    }

    pub fn get(&self, index: u64) -> Option<u64> {
        // Only recognize valid index range
        let last = self.last_log_index?;
        let first = self.first_log_index?;
        if index < first || index > last {
            return None;
        }
        // Find the last boundary with start <= index
        let slice = &self.boundaries;
        let pos = slice
            .binary_search_by(|&(start, _)| {
                if start > index {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .unwrap_or_else(|x| x);

        if pos == 0 {
            None
        } else {
            Some(slice[pos - 1].1)
        }
    }

    pub fn truncate(&mut self, index: u64) {
        while let Some(&(start, _)) = self.boundaries.front() {
            if start < index {
                self.boundaries.pop_front();
            } else {
                break;
            }
        }

        if let Some(first) = self.first_log_index {
            if first < index {
                self.first_log_index = self.boundaries.front().map(|&(start, _)| start);
            }
        }
        if let Some(last) = self.last_log_index {
            if last < index {
                self.first_log_index = None;
                self.last_log_index = None;
                self.boundaries.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENTRIES: &[(u64, u64)] = &[(5, 5), (6, 5), (7, 6), (8, 7), (9, 7), (10, 7), (11, 7)];
    fn new_test_term_cache() -> TermCache {
        let mut tc = TermCache::new();
        for (index, term) in ENTRIES {
            tc.insert(*index, *term);
        }
        tc
    }

    #[test]
    fn test_term_cache_get() {
        let tc = new_test_term_cache();

        for (index, expect_term) in ENTRIES {
            let term = tc.get(*index).unwrap();
            assert_eq!(term, *expect_term);
        }

        // last_log_index is 11
        assert_eq!(tc.get(12), None);
        assert_eq!(tc.get(4), None);
    }

    #[test]
    fn test_overwriting_behavior() {
        let mut cache = new_test_term_cache();

        // Insert (9, 8) - this should remove (9, 7), (10, 7), (11, 7) and add (9, 8)
        cache.insert(9, 8);

        // Verify the overwriting behavior
        assert_eq!(cache.get(8), Some(7)); // Should still exist
        assert_eq!(cache.get(9), Some(8)); // New entry
        assert_eq!(cache.get(10), None); // Should be removed
        assert_eq!(cache.get(11), None); // Should be removed

        // Verify all remaining entries
        assert_eq!(cache.get(5), Some(5));
        assert_eq!(cache.get(6), Some(5));
        assert_eq!(cache.get(7), Some(6));
        assert_eq!(cache.get(8), Some(7));
        assert_eq!(cache.get(9), Some(8));
    }

    #[test]
    fn test_truncate_basic() {
        let mut tc = new_test_term_cache();

        // truncate(8) should remove (5, 5), (6, 5), (7, 6)
        tc.truncate(8);

        // Verify truncate behavior
        assert_eq!(tc.get(5), None); // Removed
        assert_eq!(tc.get(6), None); // Removed
        assert_eq!(tc.get(7), None); // Removed
        assert_eq!(tc.get(8), Some(7)); // Still exists
        assert_eq!(tc.get(9), Some(7)); // Still exists
        assert_eq!(tc.get(10), Some(7)); // Still exists
        assert_eq!(tc.get(11), Some(7)); // Still exists
    }

    #[test]
    fn test_truncate_edge_cases() {
        let mut tc = TermCache::new();

        // Test truncate on empty cache
        tc.truncate(5);
        assert!(tc.boundaries.is_empty());

        // Add single entry and truncate it
        tc.insert(10, 5);
        tc.truncate(11);
        assert!(tc.boundaries.is_empty());

        // Add entry and truncate with same index
        tc.insert(10, 5);
        tc.truncate(10);
        assert_eq!(tc.get(10), Some(5));
        assert!(!tc.boundaries.is_empty());
    }
}
