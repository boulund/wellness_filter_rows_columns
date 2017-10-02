#!/usr/bin/env python3
# Filter rows or columns (or both) in tab separated tables
# Fredrik Boulund 2017

from sys import argv, exit
import argparse
import pandas as pd


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


def main(table_fn, min_rowsum, min_colsum, outfile):
    """Filter rows and/or columns from tab separated table.
    """

    df = pd.read_table(table_fn, 
            header=0,
            index_col=0)
    print("Read table containing", df.shape[0], "rows,", df.shape[1], "columns.")

    rowsums = df.sum(axis=1)
    row_filtered = df.loc[rowsums > min_rowsum]
    print(sum(rowsums >= min_rowsum), "rows remain after filtering")

    colsums = row_filtered.sum(axis=0)
    passing_columns = colsums > min_colsum
    col_filtered = row_filtered[passing_columns.index[passing_columns]]
    print(sum(colsums >= min_colsum), "columns remain after filtering")

    print("Writing filtered table to", outfile)
    col_filtered.to_csv(outfile, sep="\t")


if __name__ == "__main__":
    options = parse_args()
    main(options.TABLE, 
         options.rowsum, 
         options.colsum, 
         options.outfile)

