/**
 * A highly performant CSV splitting tool for the command line written in Rust.
 *
 * Author: Pranav Kumar <pmkumar@cmu.edu>
 */

use std::error::Error;
use std::fs;
use std::path::Path;

use clap::Clap;
use csv::{Reader, ByteRecord, StringRecord, StringRecordIter, Writer};

/* Output directory for generated CSVs. */
static OUTPUT_DIR: &str = "output/data";

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

fn split(file: &str, max_rows: i32, output_dir: String) -> Result<(), Box<dyn Error>> {
    /* Read file to split. */
    let filename = Path::new(file)
        .file_stem().unwrap()
        .to_str().unwrap();
    let mut record = ByteRecord::new();
    let mut rdr = Reader::from_path(file).expect("Error splitting 1");

    /* Extract headers. */
    let headers : StringRecord = rdr.headers()?.clone();

    let mut batch = 1;
    let mut has_record = rdr.read_byte_record(&mut record)?;
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
            has_record = rdr.read_byte_record(&mut record)?;
            i += 1;
        }

        /* Increment batch. */
        batch += 1;
    }
    Ok(())
}

fn main() {
    let opts: Opts = Opts::parse();

    fs::create_dir_all(&opts.output_dir).expect("Error creating output directory");
    split(&opts.file, opts.max_rows, opts.output_dir).expect("Error splitting");
}
