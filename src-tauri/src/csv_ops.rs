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
