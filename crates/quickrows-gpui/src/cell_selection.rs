use std::ops::RangeInclusive;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CellPosition {
    pub(crate) row: usize,
    pub(crate) column: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CellSelection {
    anchor: CellPosition,
    active: CellPosition,
}

impl CellSelection {
    pub(crate) fn single(row: usize, column: usize) -> Self {
        let position = CellPosition { row, column };
        Self {
            anchor: position,
            active: position,
        }
    }

    pub(crate) fn anchor(self) -> CellPosition {
        self.anchor
    }

    pub(crate) fn active(self) -> CellPosition {
        self.active
    }

    pub(crate) fn set_active(&mut self, row: usize, column: usize) {
        self.active = CellPosition { row, column };
    }

    pub(crate) fn move_to(&mut self, row: usize, column: usize, extend: bool) {
        let next = CellPosition { row, column };
        if !extend {
            self.anchor = next;
        }
        self.active = next;
    }

    pub(crate) fn rows(self) -> RangeInclusive<usize> {
        self.anchor.row.min(self.active.row)..=self.anchor.row.max(self.active.row)
    }

    pub(crate) fn columns(self) -> RangeInclusive<usize> {
        self.anchor.column.min(self.active.column)..=self.anchor.column.max(self.active.column)
    }

    pub(crate) fn contains(self, row: usize, column: usize) -> bool {
        self.rows().contains(&row) && self.columns().contains(&column)
    }

    pub(crate) fn dimensions(self) -> (usize, usize) {
        (
            self.rows().end() - self.rows().start() + 1,
            self.columns().end() - self.columns().start() + 1,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_reverse_rectangles() {
        let mut selection = CellSelection::single(10, 5);
        selection.set_active(3, 2);
        assert_eq!(selection.rows(), 3..=10);
        assert_eq!(selection.columns(), 2..=5);
        assert_eq!(selection.dimensions(), (8, 4));
        assert!(selection.contains(7, 4));
        assert!(!selection.contains(11, 4));
    }

    #[test]
    fn reverse_selection_preserves_its_anchor() {
        let mut selection = CellSelection::single(10, 5);
        selection.set_active(3, 2);

        assert_eq!(selection.anchor(), CellPosition { row: 10, column: 5 });
        assert_eq!(selection.active(), CellPosition { row: 3, column: 2 });
    }

    #[test]
    fn moving_without_extension_resets_anchor() {
        let mut selection = CellSelection::single(1, 1);
        selection.move_to(5, 4, true);
        assert_eq!(selection.dimensions(), (5, 4));
        selection.move_to(3, 2, false);
        assert_eq!(selection.dimensions(), (1, 1));
    }
}
