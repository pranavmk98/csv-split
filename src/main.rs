use std::error::Error;
use std::path::Path;

use clap::Clap;
use csv::{Reader, ByteRecord, StringRecord, StringRecordIter, Writer};

#[derive(Clap)]
#[clap(version = "1.0", author = "Pranav K. <pmkumar@cmu.edu>")]
struct Opts {
    /* Option. */
    #[clap(short, long, default_value = "1000")]
    max: i32,

    /* Required. */
    file: String,
}

fn split(file: &str, max: i32) -> Result<(), Box<dyn Error>> {
    let mut record = ByteRecord::new();
    let mut rdr = Reader::from_path(file)?;
    /* Extract headers. */
    let headers : StringRecord = rdr.headers()?.clone();

    let file = Path::new(file).file_stem().unwrap().to_str().unwrap();

    let mut batch = 1;
    let mut has_record = rdr.read_byte_record(&mut record)?;
    while has_record {
        /* Create new file and write headers. */
        let new_file : String = format!("{}-{}.csv", file, batch);
        let mut writer = Writer::from_path(new_file)?;
        let mut it : StringRecordIter = headers.iter();
        writer.write_record(&mut it)?;

        let mut i = 0;
        while i < max && has_record {
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

    // let mut rdr = Reader::from_path(FILENAME)?;
    split(&opts.file, opts.max).expect("Error splitting");
}
