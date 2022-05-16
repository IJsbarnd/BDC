#! usr/bin/env python3

"""
assignment1.py
First assignment of Big Data Computing
Calculates the average PHRED score per base position for all reads within a fastq file.
"""

_author_ = "IJsbrand Pool"
_version_ = 2.0

import argparse as ap
import multiprocessing as mp
import csv
import sys


def chunks(number, mysize):
    """ Returns the chunks. """
    mychunks = []
    for i in range(mysize):
        start = int(i * len(number) / mysize)
        end = int((i + 1) * len(number) / mysize)
        mychunks.append(number[start:end])

    return mychunks


def read_fastq(fastq_file):
    """ Reads the files"""
    quality = []
    qual = True

    with open(fastq_file, encoding='UTF-8') as fastq:
        while qual:
            header = fastq.readline()
            nucleotides = fastq.readline()
            strand = fastq.readline()
            qual = fastq.readline().rstrip()
            if qual:
                quality.append(qual)
            if header or nucleotides or strand:
                pass

    return quality


def calculate_quals(quality):
    """ Calculates quality scores """
    results = []
    for qual in quality:
        for item, checker in enumerate(qual):
            try:
                results[item] += ord(checker) - 33
            except IndexError:
                results.append(ord(checker) - 33)
    return results


def generate_output(average_phredscores, csvfile):
    """ Generates the output for the file('s) """
    if csvfile is None:
        csv_writer = csv.writer(sys.stdout, delimiter=',')
        for i, score in enumerate(average_phredscores):
            csv_writer.writerow([i, score])

    else:
        with open(csvfile, 'w', encoding='UTF-8', newline='') as myfastq:
            csv_writer = csv.writer(myfastq, delimiter=',')
            for i, score in enumerate(average_phredscores):
                csv_writer.writerow([i, score])


if __name__ == '__main__':
    # argparse
    argparser = ap.ArgumentParser(description="Script for assignment 1 of Big Data Computing")
    argparser.add_argument("-n", action="store",
                           dest="n", required=True, type=int,
                           help="Amount of cores to be used")
    argparser.add_argument("-o", action="store", dest="CSVfile",
                           required=False, help="CSV file to save the output.")
    argparser.add_argument("fastq_files", action="store",
                           nargs='+', help="At least 1 ILLUMINA fastq file to process")
    args = argparser.parse_args()

    for file in args.fastq_files:
        qualities = read_fastq(file)
        qual_chunked = chunks(qualities, 4)

        with mp.Pool(args.n) as pool:
            phredscores = pool.map(calculate_quals, qual_chunked)

        phredscores_avg = [sum(i) / len(qualities) for i in zip(*phredscores)]

        if len(args.fastq_files) > 1:
            if args.CSVfile is None:
                sys.stdout.write(file + "\n")
                CSV = None
            else:
                CSV = f'{file}.{args.CSVfile}'
        else:
            CSV = args.CSVfile

        generate_output(phredscores_avg, CSV)
