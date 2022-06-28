#! usr/bin/env python3

"""
assignment4.py
"""

# Metadata.
__author__ = "IJsbrand Pool"
__version__ = 4.0

import sys
import csv


def fastq_reader(fastqfile):
    """ Reads file and calculated the score. """

    with open(fastqfile, 'r') as fastq:
        complete = True
        counter = 0
        length_min = 1e99
        length_max = 0
        length_average = [0, 0]
        while True:
            header = fastq.readline().rstrip()
            nucleotides = fastq.readline().rstrip()
            seperator = fastq.readline().rstrip()
            qual = fastq.readline().rstrip()

            if len(header) == 0:
                break
            if len(nucleotides) == 0:
                counter += 1
                break
            if len(seperator) == 0:
                counter += 2
                break
            if len(qual) == 0:
                counter += 3
                break
            else:
                counter += 4

            if complete:
                if len(qual) != len(nucleotides):
                    complete = False
                if not header.startswith('@'):
                    complete = False

            if len(nucleotides) < length_min:
                length_min = len(nucleotides)
            if len(nucleotides) > length_max:
                length_max = len(nucleotides)
            length_average[0] += len(nucleotides)
            length_average[1] += 1

        file = fastqfile.split('/')[-1]
        return {'file': file, 'complete': complete, 'length_min': length_min,
                'length_max': length_max, 'length_average': length_average[0] / length_average[1]}


if __name__ == '__main__':
    result = fastq_reader(sys.argv[1])
    csv.writer(sys.stdout, delimiter=',').writerow(
        [result['file'], result['complete'], result['length_min'],
         result['length_max'], result['length_average']])