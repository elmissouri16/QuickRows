//! RFC 7111 URI fragment identifiers for `text/csv` entities.
//!
//! Coordinates are parsed as one-based RFC positions and resolved to zero-based
//! ranges. CSV row coordinates include the optional header record because RFC
//! 7111 addresses the complete CSV entity, not QuickRows' data-row projection.

use std::fmt;
use std::ops::RangeInclusive;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FragmentPosition {
    Number(usize),
    Last,
}

impl FromStr for FragmentPosition {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "*" {
            return Ok(Self::Last);
        }
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(format!("Invalid CSV fragment position: {value}"));
        }
        value
            .parse::<usize>()
            .map(Self::Number)
            .map_err(|_| format!("CSV fragment position is too large: {value}"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FragmentSpan {
    pub start: FragmentPosition,
    pub end: FragmentPosition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FragmentCellSpan {
    pub start_row: FragmentPosition,
    pub start_column: FragmentPosition,
    pub end_row: FragmentPosition,
    pub end_column: FragmentPosition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CsvFragment {
    Rows(Vec<FragmentSpan>),
    Columns(Vec<FragmentSpan>),
    Cells(Vec<FragmentCellSpan>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedFragmentRegion {
    Rows(RangeInclusive<usize>),
    Columns(RangeInclusive<usize>),
    Cells {
        rows: RangeInclusive<usize>,
        columns: RangeInclusive<usize>,
    },
}

impl fmt::Display for FragmentPosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(value) => write!(formatter, "{value}"),
            Self::Last => formatter.write_str("*"),
        }
    }
}

impl fmt::Display for FragmentSpan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start == self.end {
            write!(formatter, "{}", self.start)
        } else {
            write!(formatter, "{}-{}", self.start, self.end)
        }
    }
}

impl fmt::Display for FragmentCellSpan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start_row == self.end_row && self.start_column == self.end_column {
            write!(formatter, "{},{}", self.start_row, self.start_column)
        } else {
            write!(
                formatter,
                "{},{}-{},{}",
                self.start_row, self.start_column, self.end_row, self.end_column
            )
        }
    }
}

impl fmt::Display for CsvFragment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rows(spans) => {
                formatter.write_str("row=")?;
                for (index, span) in spans.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(";")?;
                    }
                    write!(formatter, "{span}")?;
                }
            }
            Self::Columns(spans) => {
                formatter.write_str("col=")?;
                for (index, span) in spans.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(";")?;
                    }
                    write!(formatter, "{span}")?;
                }
            }
            Self::Cells(spans) => {
                formatter.write_str("cell=")?;
                for (index, span) in spans.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(";")?;
                    }
                    write!(formatter, "{span}")?;
                }
            }
        }
        Ok(())
    }
}

fn parse_span(value: &str) -> Result<FragmentSpan, String> {
    let mut parts = value.split('-');
    let start = parts
        .next()
        .ok_or_else(|| "CSV fragment span is empty".to_string())?
        .parse()?;
    let end = parts.next().map(str::parse).transpose()?.unwrap_or(start);
    if parts.next().is_some() {
        return Err(format!("Invalid CSV fragment span: {value}"));
    }
    Ok(FragmentSpan { start, end })
}

fn parse_cell(value: &str) -> Result<(FragmentPosition, FragmentPosition), String> {
    let mut parts = value.split(',');
    let row = parts
        .next()
        .ok_or_else(|| "CSV cell row is missing".to_string())?
        .parse()?;
    let column = parts
        .next()
        .ok_or_else(|| "CSV cell column is missing".to_string())?
        .parse()?;
    if parts.next().is_some() {
        return Err(format!("Invalid CSV cell position: {value}"));
    }
    Ok((row, column))
}

fn parse_cell_span(value: &str) -> Result<FragmentCellSpan, String> {
    let mut parts = value.split('-');
    let start = parse_cell(
        parts
            .next()
            .ok_or_else(|| "CSV cell selection is empty".to_string())?,
    )?;
    let end = parts.next().map(parse_cell).transpose()?.unwrap_or(start);
    if parts.next().is_some() {
        return Err(format!("Invalid CSV cell span: {value}"));
    }
    Ok(FragmentCellSpan {
        start_row: start.0,
        start_column: start.1,
        end_row: end.0,
        end_column: end.1,
    })
}

impl FromStr for CsvFragment {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.strip_prefix('#').unwrap_or(value);
        let (kind, selections) = value
            .split_once('=')
            .ok_or_else(|| "CSV fragment must contain a selector and '='".to_string())?;
        if selections.is_empty() {
            return Err("CSV fragment selection is empty".to_string());
        }
        let selections = selections.split(';').collect::<Vec<_>>();
        if selections.iter().any(|selection| selection.is_empty()) {
            return Err("CSV fragment contains an empty selection".to_string());
        }
        match kind {
            "row" => selections
                .into_iter()
                .map(parse_span)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Rows),
            "col" => selections
                .into_iter()
                .map(parse_span)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Columns),
            "cell" => selections
                .into_iter()
                .map(parse_cell_span)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Cells),
            _ => Err(format!("Unsupported CSV fragment selector: {kind}")),
        }
    }
}

fn raw_position(position: FragmentPosition, maximum: usize) -> Option<usize> {
    match position {
        FragmentPosition::Number(0) => None,
        FragmentPosition::Number(value) => Some(value - 1),
        FragmentPosition::Last => maximum.checked_sub(1),
    }
}

fn resolve_span(span: FragmentSpan, maximum: usize) -> Option<RangeInclusive<usize>> {
    let start = raw_position(span.start, maximum)?;
    let end = raw_position(span.end, maximum)?;
    if start >= maximum || start > end {
        return None;
    }
    Some(start..=end.min(maximum - 1))
}

impl CsvFragment {
    pub fn resolve(&self, row_count: usize, column_count: usize) -> Vec<ResolvedFragmentRegion> {
        match self {
            Self::Rows(spans) => spans
                .iter()
                .filter_map(|span| resolve_span(*span, row_count).map(ResolvedFragmentRegion::Rows))
                .collect(),
            Self::Columns(spans) => spans
                .iter()
                .filter_map(|span| {
                    resolve_span(*span, column_count).map(ResolvedFragmentRegion::Columns)
                })
                .collect(),
            Self::Cells(spans) => spans
                .iter()
                .filter_map(|span| {
                    let rows = resolve_span(
                        FragmentSpan {
                            start: span.start_row,
                            end: span.end_row,
                        },
                        row_count,
                    )?;
                    let columns = resolve_span(
                        FragmentSpan {
                            start: span.start_column,
                            end: span.end_column,
                        },
                        column_count,
                    )?;
                    Some(ResolvedFragmentRegion::Cells { rows, columns })
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_rfc_7111_selector_forms() {
        assert_eq!(
            "#row=4;5-7;8-*".parse::<CsvFragment>().unwrap(),
            CsvFragment::Rows(vec![
                FragmentSpan {
                    start: FragmentPosition::Number(4),
                    end: FragmentPosition::Number(4),
                },
                FragmentSpan {
                    start: FragmentPosition::Number(5),
                    end: FragmentPosition::Number(7),
                },
                FragmentSpan {
                    start: FragmentPosition::Number(8),
                    end: FragmentPosition::Last,
                },
            ])
        );
        assert!(matches!(
            "col=1-2;*".parse::<CsvFragment>().unwrap(),
            CsvFragment::Columns(spans) if spans.len() == 2
        ));
        assert!(matches!(
            "cell=4,1;4,1-6,2".parse::<CsvFragment>().unwrap(),
            CsvFragment::Cells(spans) if spans.len() == 2
        ));
    }

    #[test]
    fn parsed_fragments_round_trip_through_display() {
        for (value, canonical) in [
            ("row=1;3-5;*-*", "row=1;3-5;*"),
            ("col=1-2;4", "col=1-2;4"),
            ("cell=2,1-3,4;5,6", "cell=2,1-3,4;5,6"),
        ] {
            let fragment = value.parse::<CsvFragment>().unwrap();
            assert_eq!(fragment.to_string(), canonical);
            assert_eq!(
                fragment.to_string().parse::<CsvFragment>().unwrap(),
                fragment
            );
        }
    }

    #[test]
    fn rejects_rfc_7111_syntax_errors() {
        for value in [
            "ROW=1",
            "row=",
            "row=1;",
            "row=a",
            "row=1-2-3",
            "cell=1",
            "cell=1,2,3",
            "cell=1,2-3",
        ] {
            assert!(value.parse::<CsvFragment>().is_err(), "{value}");
        }
    }

    #[test]
    fn resolves_clamps_and_ignores_invalid_regions_per_rfc_7111() {
        let fragment = "row=1-2;5-4;13-16;6-*".parse::<CsvFragment>().unwrap();
        assert_eq!(
            fragment.resolve(8, 3),
            vec![
                ResolvedFragmentRegion::Rows(0..=1),
                ResolvedFragmentRegion::Rows(5..=7),
            ]
        );

        let cells = "cell=2,2-20,9;8,3-2,1;0,1".parse::<CsvFragment>().unwrap();
        assert_eq!(
            cells.resolve(8, 3),
            vec![ResolvedFragmentRegion::Cells {
                rows: 1..=7,
                columns: 1..=2,
            }]
        );
    }
}
