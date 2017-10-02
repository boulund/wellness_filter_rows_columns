#!/usr/bin/env python3
# Filter rows or columns (or both) in tab separated tables
# Fredrik Boulund 2017

from sys import argv, exit
import argparse

global filtered_rows

def parse_args():
    """Parse command line arguments.
    """

    desc = """Filter rows or columns from tab separated tables.
           Assumes first row contains headers, and that the first column is the index colum
           Fredrik Boulund 2017. """
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("TABLE", metavar="FILE",
            help="Table to be processed.")
    parser.add_argument("-o", "--outfile", metavar="OUTFILE",
            default="filtered_table.tsv",
            help="Output filename [%(default)s].")
    parser.add_argument("-r", "--rowsum", metavar="R",
            default=5,
            type=float,
            help="Minimum row sum to include row in output [%(default)s].")
    parser.add_argument("-c", "--colsum", metavar="C",
            default=5,
            type=float,
            help="Minimum column sum to include row in output [%(default)s].")

    if len(argv) < 2:
        parser.print_help()
        exit(1)

    return parser.parse_args()


def yield_rows(filename, separator="\t"):
    """Yield rows from filename.
    """
    with open(filename) as f:
        for line in f:
            yield line.strip().split(separator)


class Row_filterer():
    """Yield rows that have rowsum >= min_rowsum.
    """
    def __init__(self):
        self.filtered_rows = 0

    def filter_rows(self, rows, min_rowsum):
        for row in rows:
            rowsum = sum(int(v) for v in row[1:])
            if rowsum >= min_rowsum:
                yield row
            else:
                #print("Skipping row", row[0], "Rowsum", rowsum)
                self.filtered_rows += 1


def main(table_fn, min_rowsum, min_colsum, outfile, separator="\t"):
    """Filter rows from tab separated table.
    """

    rows = yield_rows(table_fn)
    header_line = next(rows)

    row_filterer = Row_filterer()
    filtered_rows = row_filterer.filter_rows(rows, min_rowsum)

    print("Writing filtered table to", outfile)
    with open(outfile, 'w') as out:
        out.write(separator.join(header_line)+"\n")
        for filtered_row in filtered_rows:
            out.write(separator.join(str(v) for v in filtered_row)+"\n")
    print("Filtered", row_filterer.filtered_rows, "rows.")


if __name__ == "__main__":
    options = parse_args()
    main(options.TABLE,
         options.rowsum,
         options.colsum,
         options.outfile)
