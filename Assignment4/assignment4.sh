#!/bin/bash

export DATA=/data/dataprocessing/rnaseq_data/Brazil_Brain/
export WORKDIR=/homes/jlaeisinga/Desktop/test

echo Filenaam,Valide,Min_length,Max_length,Average_length
find $DATA -name '*.fastq' | parallel --workdir $WORKDIR -j2 --results ./ --sshloginfile myhosts.txt $WORKDIR/assignment4.py {}