#!/bin/bash 

# command line usage: bash run8_parallel.sh

INPUT=data/inputs/data_2tran_header.csv # CHANGE THIS
OUTPUT=data/outputs/may26_out_data.csv #CHANGE THIS
RUN_SCRIPT=run8.py # CHANGE THIS
MAX_FILES=25
ENDING=.csv

current_epoch=$(date +%s)
newline=$'\n'

# process input files
header=$(cat $INPUT | head -1)
cat $INPUT | tail -n +2 > _tmp_remove_header.csv

# process output files
output_stripped=${OUTPUT%.*}
number_of_lines=($(wc _tmp_remove_header.csv))

# split input file
total_per_output=$((number_of_lines / $MAX_FILES))
total_per_output=$((total_per_output+1))
split -l $total_per_output _tmp_remove_header.csv

# run model for each input file on separate process threads
for f in xa*
do
	echo "$header$newline$(cat $f)" > ${f}_with_header
	python $RUN_SCRIPT --data ${f}_with_header --fraud-list data/fraud_list.csv --output $output_stripped${f}_with_header$ENDING --csv-delimiter \| --no-signals &
done

wait
rm _tmp_remove_header.csv
rm xa*

# get header of output
#echo $output_stripped'xaa'_with_header$ENDING
head -1 $output_stripped'xaa'_with_header$ENDING > data/outputs/${current_epoch}.csv
tail -n +2 -q $output_stripped'xa'*_with_header$ENDING >> data/outputs/${current_epoch}.csv

# named final file as epoch time (for later)
cp data/outputs/${current_epoch}.csv $OUTPUT
rm data/outputs/${current_epoch}.csv

rm $output_stripped'xa'*_with_header$ENDING