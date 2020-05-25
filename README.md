# csv-split

csv-split is a high performance CSV Splitter built in Rust. Helpful for quickly
splitting CSVs into multiple files from the command line.

This was built as a weekend project with the following goals in mind:

1. Learn Rust
2. Create a somewhat useful tool.

Currently, [xsv](https://github.com/BurntSushi/xsv) is one of the best tools out
there for CSV processing, and is fantastic. Given the amount of development that has
gone into this, it is therefore surprising that `csv-split` can give `xsv` a run
for its money on smaller inputs. Given that `csv-split` loads the entire file into
memory (at least in parallel mode, which can be disabled with `--no-parallel`),
it is predictably much slower on "large" (500+ MB) files.

## Installation

Building this project from source requires [Cargo](https://crates.io/install) and
can be done as follows:

```
git clone git@github.com/pranavmk98/csv-split
cd csv-split
cargo build --release
```

Compilation will likely take a few minutes. The binary will end up at `./target/release/csv-split`.

## Usage

`csv-split data.csv --max-rows 500`

## Performance

Some rough benchmarking was performed using the `worldcitiespop.csv` dataset from
the [Data Science Toolkit project](https://github.com/petewarden/dstkdata/), which
is about 125MB and contains approximately 2.7 million rows.

The compared splitters were [xsv](https://github.com/BurntSushi/xsv) (written
in Rust) and a [CSV splitter](https://github.com/PerformanceHorizonGroup/csv-split)
by PerformanceHorizonGroup (written in C).

These benchmarks ran on my admittedly underpowered machine with an Intel i5-8250U
(4 Cores, 8 Threads) and 8GB of memory.

`csv-split`: `0.36s user 0.36s system 170% cpu 0.428 total`
`xsv`: `0.58s user 0.09s system 99% cpu 0.666 total`

## Future Work

Ideally, the entire CSV would not be loaded into memory at once. Once I learn some
more about concurrency in Rust, I would like to take a stab at a more threadpool
like structure, firing off a thread for each batch to process.

Of course, PRs are always welcome :)
