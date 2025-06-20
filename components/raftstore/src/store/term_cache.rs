use std::collections::BTreeMap;

/// TermCache maps raft log index to its raft log term.
/// E.g., there are logs in the format (index, term):
/// (5, 5), (6, 5), (7, 6), (8, 7), (9, 7), (10, 7), (11, 7)
/// TermCache efficiently map [5, 5] to 5, and 7 to 6, and [8, 11] to 7 in
/// `O(term)` space complexity.
#[derive(Debug, Clone)]
pub struct TermCache {
    // Maps the starting index of each term range to the term value
    // Key: starting index, Value: term
    term_ranges: BTreeMap<u64, u64>,
}

impl TermCache {
    /// Creates a new empty TermCache
    pub fn new() -> Self {
        Self {
            term_ranges: BTreeMap::new(),
        }
    }

    /// Adds a range of indices [start_index, end_index] that all have the same
    /// term Overwrites any existing entries with indices >= start_index
    pub fn insert_range(&mut self, start_index: u64, end_index: u64, term: u64) {
        assert!(
            start_index <= end_index,
            "Invalid range: start_index must be <= end_index"
        );

        // Remove all entries with indices >= start_index (overwrite behavior)
        self.truncate_at(start_index);

        // Insert the new range
        self.term_ranges.insert(start_index, term);
    }

    /// Adds a single index-term mapping
    /// Overwrites any existing entries with indices >= index
    pub fn insert(&mut self, index: u64, term: u64) {
        // Remove all entries with indices >= index (overwrite behavior)
        self.truncate_at(index);

        // Try to extend the previous range if it has the same term
        if let Some((&prev_start, &prev_term)) = self.term_ranges.range(..index).next_back() {
            let prev_end = self.find_range_end(prev_start);

            // If the previous range ends exactly at index-1 and has the same term,
            // we can extend it instead of creating a new range
            if prev_end == index - 1 && prev_term == term {
                // The new entry extends the previous range, no need to insert a new range
                return;
            }
        }

        // Insert the new single entry as a range
        self.term_ranges.insert(index, term);
    }

    /// Removes all entries with indices >= from_index
    pub fn truncate_at(&mut self, from_index: u64) {
        // Find all ranges that need to be modified or removed
        let ranges_to_process: Vec<(u64, u64)> = self
            .term_ranges
            .range(..=from_index)
            .map(|(&start, &term)| (start, term))
            .collect();

        // Remove all ranges that start at or after from_index
        let keys_to_remove: Vec<u64> = self
            .term_ranges
            .range(from_index..)
            .map(|(&start, _)| start)
            .collect();

        for key in keys_to_remove {
            self.term_ranges.remove(&key);
        }

        // Check if we need to truncate any existing range
        for (range_start, term) in ranges_to_process {
            let range_end = self.find_range_end(range_start);

            // If this range extends beyond from_index, truncate it
            if range_end >= from_index && range_start < from_index {
                // Remove the original range and insert the truncated version
                self.term_ranges.remove(&range_start);
                self.term_ranges.insert(range_start, term);
                break; // There should only be one such range
            }
        }
    }

    /// Gets the term for a given index, returns None if not found
    pub fn get(&self, index: u64) -> Option<u64> {
        // Find the largest starting index that is <= our target index
        self.term_ranges
            .range(..=index)
            .next_back()
            .and_then(|(&range_start, &term)| {
                let range_end = self.find_range_end(range_start);
                if index <= range_end { Some(term) } else { None }
            })
    }

    /// Helper function to find the end index of a range starting at start_index
    fn find_range_end(&self, start_index: u64) -> u64 {
        // Find the next range start, the current range ends just before it
        self.term_ranges
            .range((start_index + 1)..)
            .next()
            .map(|(&next_start, _)| next_start - 1)
            .unwrap_or(u64::MAX) // If no next range, this range extends to infinity
    }

    /// Returns the number of term ranges stored (useful for debugging)
    pub fn num_ranges(&self) -> usize {
        self.term_ranges.len()
    }

    /// Creates a TermCache from a vector of (index, term) pairs
    /// This is optimized to create ranges automatically
    pub fn from_entries(entries: Vec<(u64, u64)>) -> Self {
        let mut cache = Self::new();

        if entries.is_empty() {
            return cache;
        }

        let mut sorted_entries = entries;
        sorted_entries.sort_by_key(|(index, _)| *index);

        let mut range_start = sorted_entries[0].0;
        let mut current_term = sorted_entries[0].1;

        for i in 1..sorted_entries.len() {
            let (index, term) = sorted_entries[i];

            // If term changes or there's a gap in indices, end current range
            if term != current_term || index != sorted_entries[i - 1].0 + 1 {
                // Insert the completed range
                cache.term_ranges.insert(range_start, current_term);

                // Start new range
                range_start = index;
                current_term = term;
            }
        }

        // Insert the final range
        cache.term_ranges.insert(range_start, current_term);

        cache
    }

    /// Returns all (index, term) pairs currently stored in the cache
    /// Useful for debugging and testing
    pub fn to_entries(&self) -> Vec<(u64, u64)> {
        let mut entries = Vec::new();

        for (&range_start, &term) in &self.term_ranges {
            let range_end = self.find_range_end(range_start);

            // Handle the case where range extends to u64::MAX
            let actual_end = if range_end == u64::MAX {
                // For practical purposes, limit to a reasonable number for display
                std::cmp::min(range_start + 100, u64::MAX)
            } else {
                range_end
            };

            for index in range_start..=actual_end {
                entries.push((index, term));
                if index == u64::MAX {
                    break;
                } // Prevent overflow
            }
        }

        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overwriting_behavior() {
        // Start with: (5, 5), (6, 5), (7, 6), (8, 7), (9, 7), (10, 7), (11, 7)
        let entries = vec![(5, 5), (6, 5), (7, 6), (8, 7), (9, 7), (10, 7), (11, 7)];

        let mut cache = TermCache::from_entries(entries);

        // Verify initial state
        assert_eq!(cache.get(9), Some(7));
        assert_eq!(cache.get(10), Some(7));
        assert_eq!(cache.get(11), Some(7));

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
    fn test_overwriting_with_range() {
        let mut cache = TermCache::new();

        // Insert initial ranges
        cache.insert_range(5, 10, 5);
        cache.insert_range(11, 15, 6);

        // Verify initial state
        assert_eq!(cache.get(8), Some(5));
        assert_eq!(cache.get(12), Some(6));

        // Insert range (8, 12, 7) - should overwrite everything from 8 onwards
        cache.insert_range(8, 12, 7);

        // Verify results
        assert_eq!(cache.get(5), Some(5)); // Unchanged
        assert_eq!(cache.get(7), Some(5)); // Unchanged
        assert_eq!(cache.get(8), Some(7)); // New range
        assert_eq!(cache.get(10), Some(7)); // New range
        assert_eq!(cache.get(12), Some(7)); // New range
        assert_eq!(cache.get(13), None); // Removed
        assert_eq!(cache.get(15), None); // Removed
    }

    #[test]
    fn test_range_extension() {
        let mut cache = TermCache::new();

        // Insert range [5, 8] with term 5
        cache.insert_range(5, 8, 5);
        assert_eq!(cache.num_ranges(), 1);

        // Insert (9, 5) - should extend the existing range
        cache.insert(9, 5);
        assert_eq!(cache.num_ranges(), 1);
        assert_eq!(cache.get(9), Some(5));

        // Insert (10, 6) - should create a new range and truncate
        cache.insert(10, 6);
        assert_eq!(cache.get(9), Some(5));
        assert_eq!(cache.get(10), Some(6));
        assert_eq!(cache.get(11), None);
    }

    #[test]
    fn test_truncate_at() {
        let entries = vec![(5, 5), (6, 5), (7, 6), (8, 7), (9, 7), (10, 7), (11, 7)];

        let mut cache = TermCache::from_entries(entries);

        // Truncate at index 9
        cache.truncate_at(9);

        // Verify that entries 9, 10, 11 are removed
        assert_eq!(cache.get(8), Some(7));
        assert_eq!(cache.get(9), None);
        assert_eq!(cache.get(10), None);
        assert_eq!(cache.get(11), None);
    }

    #[test]
    fn test_sequential_overwrites() {
        let mut cache = TermCache::new();

        // Build initial state
        cache.insert_range(1, 5, 1);

        // Simulate raft log overwrites
        cache.insert(3, 2); // Overwrites [3, 5] with term 1, adds (3, 2)
        assert_eq!(cache.get(2), Some(1));
        assert_eq!(cache.get(3), Some(2));
        assert_eq!(cache.get(4), None);

        cache.insert(4, 2); // Extends the term 2 range
        assert_eq!(cache.get(4), Some(2));

        cache.insert(5, 3); // New term, truncates at 5
        assert_eq!(cache.get(4), Some(2));
        assert_eq!(cache.get(5), Some(3));
    }

    #[test]
    fn test_example_case() {
        // Test case from the problem: (5, 5), (6, 5), (7, 6), (8, 7), (9, 7), (10, 7),
        // (11, 7)
        let entries = vec![(5, 5), (6, 5), (7, 6), (8, 7), (9, 7), (10, 7), (11, 7)];

        let cache = TermCache::from_entries(entries);

        // Test individual lookups
        assert_eq!(cache.get(5), Some(5));
        assert_eq!(cache.get(6), Some(5));
        assert_eq!(cache.get(7), Some(6));
        assert_eq!(cache.get(8), Some(7));
        assert_eq!(cache.get(9), Some(7));
        assert_eq!(cache.get(10), Some(7));
        assert_eq!(cache.get(11), Some(7));

        // Test non-existent indices
        assert_eq!(cache.get(4), None);
        assert_eq!(cache.get(12), None);

        // Verify space efficiency
        assert_eq!(cache.num_ranges(), 3);
    }
}
