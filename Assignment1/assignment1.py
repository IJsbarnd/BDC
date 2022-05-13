#!/usr/bin/env python3


import argparse as ap
import multiprocessing as mp
import sys



argparser = ap.ArgumentParser(description="Script voor Opdracht 1 van Big Data Computing")
argparser.add_argument("-n", action="store",
                       dest="n", required=True, type=int,
                       help="Aantal cores om te gebruiken.")
argparser.add_argument("--output","-o",
                       required=False, help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
argparser.add_argument("fastq_files", action="store", nargs='+', help="Minstens 1 Illumina Fastq Format file om te verwerken")
args = argparser.parse_args()



def doeIets(fastqfile, csvfile, start=0, chunk=0):
    with open(fastqfile, 'r', encoding='utf8') as fastq:
        if chunk == 0:
            chunk = len(fastq.readlines()) -1
        fastq.seek(0)
        #fastforward
        #start = 0
        i = 0
        while i < start:
            fastq.readline()
            i += 1

        results = []
        counter = 0
        while counter < chunk:
            header = fastq.readline()
            nucleotides = fastq.readline()
            strand = fastq.readline()
            qual = fastq.readline()
            counter += 4

            if not(qual):
                # we reached the end of the file
                break
            for j, c in enumerate(qual):

                try:
                    results[j] += ord(c) - 33
                except IndexError:
                    results.append(ord(c) - 33)

    iets = [(phredscore / (counter / 4)) for phredscore in results]
    print(iets)
    with open(csvfile, 'w') as csv:
        for number in range(len(iets) - 1):
            csv.write(str(number) + ',' + str(iets[number]) + "\n")


if __name__ == '__main__':
    fastqfiles = args.fastq_files
    csv = args.output
    for file in fastqfiles:
        doeIets(file, csv)
    sys.exit(0)