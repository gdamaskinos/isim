#!/bin/bash

if [ "$1" = "" ] || [ "$2" = "" ] || [ "$3" = "" ]; then
        echo "usage: $0 <input file> <output dir> <output_size>"
        exit 1
fi

N=$(wc -l < $1)

X=$((N-$3)) 
#X=$((N*80/100))
Y=$((N-X))

head -n $X $1 > ${2}/trainingSet.csv
tail -n $3 $1 > ${2}/testSet.csv

#split -l $X $1
#mv xaa ${2}/trainingSet.csv
#mv xab ${2}/testSet.csv
 
