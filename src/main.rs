/*
 * A highly performant CSV splitting tool for the command line written in Rust.
 *
 * Author: Pranav Kumar <pmkumar@cmu.edu>
 */

use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Write, BufWriter};
use std::path::Path;

use bytelines::*;
use clap::Clap;
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

    /* Option. */
    #[clap(short, long, about = "Run splitting sequentially")]
    no_parallel: bool,

    /* Required. */
    file: String,
}

fn write_bytes<W: Write>(writer : &mut W, bytes : &[u8]) {
    writer.write(bytes).expect("Error writing");
    writer.write(&[LF]).expect("Error writing");
}

fn get_filename(output_dir: &str, filename: &str, batch: usize) -> String {
    return format!("{}/{}-{}.csv", output_dir, filename, batch);
}

fn write_batch<R: BufRead>(
    tup: (usize, &mut bytelines::ByteLinesIter<R>),
    filename: &str,
    output_dir: &str,
    max_rows : i32,
    header: &[u8]
) -> bool {
    /* Extract batch ID and records. */
    let (batch, records) = tup;

    /* Create new file. */
    let new_file : String = get_filename(&output_dir, filename, batch);
    let file = File::create(&new_file).expect("Error creating file");
    let mut writer = BufWriter::new(file);

    /* Write header. */
    write_bytes(&mut writer, header);

    /* Write up to max_rows records. */
    let mut i = 0;
    while i < max_rows {
        let record = records.next();
        if record.is_some() {
            write_bytes(&mut writer, record.unwrap().as_ref().unwrap());
            i += 1;
        } else {
            return true;
        }
    }
    false
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
    let og_file = File::open(file).unwrap();
    let reader = BufReader::new(&og_file);
    let mut records = reader.byte_lines_iter();

    /* Read header. */
    let header : Vec<u8> = records.next().unwrap()?;

    let mut batch = 1;
    let mut is_done = false;
    while !is_done {
        is_done = write_batch(
            (batch, &mut records), filename, &output_dir, max_rows, &header
        );
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
    let new_file : String = get_filename(&output_dir, filename, batch);
    let file = File::create(&new_file).expect("Error creating file");
    let mut writer = BufWriter::new(file);

    /* Write header. */
    write_bytes(&mut writer, header);

    /* Write all records. */
    for record in records {
        write_bytes(&mut writer, record.as_ref().unwrap());
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

    match opts.no_parallel {
        /* Sequential split. */
        true  => {
            split_seq(
                &opts.file, opts.max_rows, opts.output_dir
            ).expect("Error while splitting")
        }

        /* Parallelize split. */
        false => {
            split_par(
                &opts.file, opts.max_rows, opts.output_dir
            ).expect("Error while splitting")
        }
    }
}
