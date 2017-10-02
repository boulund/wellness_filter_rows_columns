#!/usr/bin/env python3
# Filter rows or columns (or both) in tab separated tables
# Fredrik Boulund 2017

from sys import argv, exit
from itertools import chain
import argparse
import numpy as np

global filtered_rows

def parse_args():
    """Parse command line arguments.
    """

    desc = """Filter rows or columns from tab separated tables.
           Assumes first row contains headers, and that the first column is the index column.
           Fredrik Boulund 2017. """
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("TABLE", metavar="FILE",
            help="Table to be processed.")
    parser.add_argument("-o", "--outfile", metavar="OUTFILE",
            default="filtered_table.tsv",
            help="Output filename [%(default)s].")
    parser.add_argument("-r", "--rowsum", metavar="R",
            default=-1,
            type=float,
            help="Minimum row sum to include row in output, "
                 "-1 skips row filtering [%(default)s].")
    parser.add_argument("-c", "--colsum", metavar="C",
            default=-1,
            type=float,
            help="Minimum column sum to include row in output, "
                 "-1 skips column filtering [%(default)s].")

    if len(argv) < 2:
        parser.print_help()
        exit(1)

    return parser.parse_args()


def yield_rows(filename, separator="\t"):
    """Yield rows from filename.
    """
    with open(filename) as f:
        yield f.readline().rstrip().split(separator)
        for line in f:
            yield line.strip().split(separator)


class Column_filterer():
    """Filter columns in a two-pass operation.
    """
    def __init__(self, num_columns):
        self.filtered_columns = 0
        self.num_columns = num_columns

    def compute_colsums(self, rows, min_colsum):
        self.col_sums = np.zeros(self.num_columns, dtype=np.int32)
        for row in rows:
            self.col_sums += np.array(row[1:], dtype=np.int32)
        self.keep_cols = self.col_sums >= min_colsum

    def filter_columns(self, rows):
        for row in rows:
            value_columns = np.array(row[1:], dtype=np.int32)
            new_line = (row[0],), value_columns[self.keep_cols]
            yield chain(*new_line) 


class Row_filterer():
    """Yield rows that have rowsum >= min_rowsum.
    """
    def __init__(self):
        self.filtered_rows = 0
        self.removed_rows = []

    def filter_rows(self, rows, min_rowsum):
        for row in rows:
            rowsum = sum(int(v) for v in row[1:])
            if rowsum >= min_rowsum:
                yield row
            else:
                self.filtered_rows += 1
                self.removed_rows.append(row[0])


def main(table_fn, min_rowsum, min_colsum, outfile, separator="\t"):
    """Filter rows and/or columns from tab separated table.
    """

    if min_rowsum > -1 and min_colsum == -1:
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
        print("Filtered row(s):\n", "\n".join(row_filterer.removed_rows))
    elif min_colsum > -1 and min_rowsum == -1:
        # Two-pass column filtering
        rows2 = yield_rows(table_fn)
        num_columns = len(next(rows2))-1
        column_filterer = Column_filterer(num_columns)
        column_filterer.compute_colsums(rows2, min_colsum)

        rows3 = yield_rows(table_fn)
        column_headers = np.array(next(rows3))
        if not column_headers[0]:
            column_headers[0] = " " # Workaround for empty first column title
        with open(outfile, 'w') as out:
            out.write("\t".join(chain(column_headers[0], 
                    column_headers[1:][column_filterer.keep_cols]))+"\n")
            for row in column_filterer.filter_columns(rows3):
                out.write("\t".join(str(v) for v in row)+"\n")
        print("Filtered", column_filterer.num_columns - sum(column_filterer.keep_cols), "columns.")
        print("Filtered column(s):\n", "\n".join(column_headers[1:][np.invert(column_filterer.keep_cols)]))
    elif (min_colsum > -1 and min_rowsum > -1) or (min_colsum == -1 and min_rowsum == -1):
        print("Code currently cannot do both row and column filtering at the same time.")


if __name__ == "__main__":
    options = parse_args()
    main(options.TABLE,
         options.rowsum,
         options.colsum,
         options.outfile)
