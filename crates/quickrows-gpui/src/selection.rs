use std::ops::RangeInclusive;

/// A compact set of row indices represented as sorted, non-overlapping ranges.
/// Adjacent ranges are merged, so selecting a million contiguous rows costs one
/// pair of `usize`s instead of a million hash-table entries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RowSelection {
    ranges: Vec<RangeInclusive<usize>>,
    len: usize,
}

impl RowSelection {
    pub(crate) fn clear(&mut self) {
        self.ranges.clear();
        self.len = 0;
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn contains(&self, row: usize) -> bool {
        self.ranges
            .binary_search_by(|range| {
                if row < *range.start() {
                    std::cmp::Ordering::Greater
                } else if row > *range.end() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    pub(crate) fn select_only(&mut self, row: usize) {
        self.ranges.clear();
        self.ranges.push(row..=row);
        self.len = 1;
    }

    pub(crate) fn select_only_range(&mut self, a: usize, b: usize) {
        let start = a.min(b);
        let end = a.max(b);
        self.ranges.clear();
        self.ranges.push(start..=end);
        self.len = end - start + 1;
    }

    pub(crate) fn select_all(&mut self, row_count: usize) {
        self.clear();
        if row_count > 0 {
            self.ranges.push(0..=row_count - 1);
            self.len = row_count;
        }
    }

    pub(crate) fn insert(&mut self, row: usize) -> bool {
        if self.contains(row) {
            return false;
        }
        self.ranges.push(row..=row);
        self.normalize();
        true
    }

    pub(crate) fn insert_range(&mut self, range: RangeInclusive<usize>) {
        if range.is_empty() {
            return;
        }
        self.ranges.push(range);
        self.normalize();
    }

    pub(crate) fn remove(&mut self, row: usize) -> bool {
        let Some(index) = self.ranges.iter().position(|range| range.contains(&row)) else {
            return false;
        };
        let start = *self.ranges[index].start();
        let end = *self.ranges[index].end();
        match (row == start, row == end) {
            (true, true) => {
                self.ranges.remove(index);
            }
            (true, false) => self.ranges[index] = start + 1..=end,
            (false, true) => self.ranges[index] = start..=end - 1,
            (false, false) => {
                self.ranges[index] = start..=row - 1;
                self.ranges.insert(index + 1, row + 1..=end);
            }
        }
        self.len -= 1;
        true
    }

    pub(crate) fn toggle(&mut self, row: usize) -> bool {
        if self.remove(row) {
            false
        } else {
            self.insert(row);
            true
        }
    }

    pub(crate) fn first(&self) -> Option<usize> {
        self.ranges.first().map(|range| *range.start())
    }

    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = usize> + '_ {
        self.ranges.iter().flat_map(|range| range.clone())
    }

    #[cfg(test)]
    pub(crate) fn ranges(&self) -> &[RangeInclusive<usize>] {
        &self.ranges
    }

    fn normalize(&mut self) {
        self.ranges.sort_unstable_by_key(|range| *range.start());
        let mut merged: Vec<RangeInclusive<usize>> = Vec::with_capacity(self.ranges.len());
        for range in self.ranges.drain(..) {
            if let Some(previous) = merged.last_mut() {
                let previous_end = *previous.end();
                if *range.start() <= previous_end.saturating_add(1) {
                    let start = *previous.start();
                    let end = previous_end.max(*range.end());
                    *previous = start..=end;
                    continue;
                }
            }
            merged.push(range);
        }
        self.len = merged
            .iter()
            .map(|range| range.end() - range.start() + 1)
            .sum();
        self.ranges = merged;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn million_row_selection_is_one_range() {
        let mut selection = RowSelection::default();
        selection.select_all(1_000_000);
        assert_eq!(selection.len(), 1_000_000);
        assert_eq!(selection.ranges(), &[0..=999_999]);
        assert!(selection.contains(500_000));
    }

    #[test]
    fn toggles_split_and_merge_ranges() {
        let mut selection = RowSelection::default();
        selection.select_only_range(10, 20);
        assert!(!selection.toggle(15));
        assert_eq!(selection.ranges(), &[10..=14, 16..=20]);
        assert!(selection.toggle(15));
        assert_eq!(selection.ranges(), &[10..=20]);
    }

    #[test]
    fn disjoint_rows_iterate_in_display_order() {
        let mut selection = RowSelection::default();
        selection.insert(9);
        selection.insert(2);
        selection.insert(5);
        assert_eq!(selection.iter().collect::<Vec<_>>(), vec![2, 5, 9]);
        selection.remove(5);
        assert_eq!(selection.first(), Some(2));
    }

    #[test]
    fn reverse_range_is_normalized() {
        let mut selection = RowSelection::default();
        selection.select_only_range(20, 10);
        assert_eq!(selection.ranges(), &[10..=20]);
        assert_eq!(selection.len(), 11);
    }
}
