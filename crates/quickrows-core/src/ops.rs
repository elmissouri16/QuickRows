use rayon::prelude::*;

pub fn search_parallel(data: &[Vec<String>], column_idx: usize, query: &str) -> Vec<usize> {
    let query_lower = query.to_lowercase();
    data.par_iter()
        .enumerate()
        .filter_map(|(idx, row)| {
            let cell = row.get(column_idx)?;
            if cell.to_lowercase().contains(&query_lower) {
                Some(idx)
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::search_parallel;

    #[test]
    fn search_parallel_finds_matching_rows() {
        let data = vec![
            vec!["alpha".to_string(), "bravo".to_string()],
            vec!["charlie".to_string(), "AlphaBeta".to_string()],
            vec!["delta".to_string(), "echo".to_string()],
        ];

        let mut matches = search_parallel(&data, 1, "alpha");
        matches.sort_unstable();
        assert_eq!(matches, vec![1]);
    }
}
