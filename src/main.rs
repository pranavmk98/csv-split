/*
 * A highly performant CSV splitting tool for the command line written in Rust.
 *
 * Author: Pranav Kumar <pmkumar@cmu.edu>
 */

use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::path::Path;

use bytelines::*;
use clap::Clap;
use csv::{Reader, ByteRecord, StringRecord, StringRecordIter, Writer};
use rayon::prelude::*;

/* Output directory for generated CSVs. */
const OUTPUT_DIR: &str = "output/data";
const LF: u8 = '\n' as u8;

#[derive(Clap)]
#[clap(version = "1.0", author = "Pranav K. <pmkumar@cmu.edu>")]
struct Opts {
    /* Option. */
    #[clap(short, long, default_value = "1000", about = "Max rows per file")]
    max_rows: i32,

    /* Option. */
    #[clap(short, long, default_value = OUTPUT_DIR, about = "Output directory for generated CSVs")]
    output_dir: String,

    /* Required. */
    file: String,
}

fn split_seq(
    file: &str,
    max_rows: i32,
    output_dir: String
) -> Result<(), Box<dyn Error>> {

    /* Read file to split. */
    let filename = Path::new(file)
        .file_stem().unwrap()
        .to_str().unwrap();
    let mut record = ByteRecord::new();
    let mut reader = Reader::from_path(file).expect("Error splitting 1");

    /* Extract headers. */
    let headers : StringRecord = reader.headers()?.clone();

    let mut batch = 1;
    let mut has_record = reader.read_byte_record(&mut record)?;
    while has_record {
        /* Create new file and write headers. */
        let new_file : String = format!("{}/{}-{}.csv", output_dir, filename, batch);
        let mut writer = Writer::from_path(new_file)?;
        let mut it : StringRecordIter = headers.iter();
        writer.write_record(&mut it)?;

        /* Write each file. */
        let mut i = 0;
        while i < max_rows && has_record {
            writer.write_byte_record(&record)?;
            has_record = reader.read_byte_record(&mut record)?;
            i += 1;
        }

        /* Increment batch. */
        batch += 1;
    }
    Ok(())
}


fn process_slice(
    tup: (usize, &mut [Result<Vec<u8>, std::io::Error>]),
    filename: &str,
    output_dir: &str,
    header: &[u8]
) {
    /* Extract batch ID and records. */
    let (batch, records) = tup;

    /* Create new file. */
    let new_file : String = format!("{}/{}-{}.csv", output_dir, filename, batch);
    let file = File::create(&new_file).expect("Error creating file");
    let mut writer = BufWriter::new(file);

    /* Write header. */
    writer.write(&header).expect("Error writing header");
    writer.write(&[LF]).expect("Error writing record");

    /* Write all records. */
    for record in records {
        writer.write(record.as_ref().unwrap()).expect("Error writing record");
        writer.write(&[LF]).expect("Error writing record");
    }
}

fn split_par(
    file: &str,
    max_rows: i32,
    output_dir: String
) -> Result<(), Box<dyn Error>> {

    /* Read file to split. */
    let filename = Path::new(file)
        .file_stem().unwrap()
        .to_str().unwrap();
    let reader = BufReader::new(File::open(file).unwrap());
    let mut iter = reader.byte_lines_iter();

    /* Read header. */
    let header : Vec<u8> = iter.next().unwrap()?;

    /* Break remaining records into chunks and operate on each. */
    let mut data : Vec<Result<Vec<u8>, std::io::Error>> = iter.collect();
    data.par_chunks_mut(max_rows as usize)
        .enumerate()
        .for_each(|slice| process_slice(slice, filename, &output_dir, &header)
    );
    Ok(())
}

fn main() {
    let opts: Opts = Opts::parse();

    /* Create output directory. */
    fs::create_dir_all(
        &opts.output_dir
    ).expect("Error creating output directory");

    /* Parallelize split. */
    split_par(&opts.file, opts.max_rows, opts.output_dir).expect("Error splitting");
}
