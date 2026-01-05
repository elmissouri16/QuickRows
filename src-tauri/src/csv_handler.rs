use csv::ReaderBuilder;
use std::fs::File;
use std::io::BufReader;

pub fn count_rows_fast(path: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(reader);
    let count = rdr.records().count();
    Ok(count)
}

pub fn get_headers(path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
    let headers = rdr
        .headers()?
        .iter()
        .map(|header| header.to_string())
        .collect();
    Ok(headers)
}

pub fn read_chunk(
    path: &str,
    start: usize,
    count: usize,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(reader);
    let rows = rdr
        .records()
        .skip(start)
        .take(count)
        .filter_map(|record| record.ok())
        .map(|record| record.iter().map(|field| field.to_string()).collect())
        .collect();
    Ok(rows)
}
