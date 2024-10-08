# Unix CLI Utilities
A collection of command line utilities designed to simplify data processing tasks in Unix environments

## xcat
xcat extends the functionality of the standard Linux cat command by including extra support for handling CSV and TSV files. 
### Features
 - Supports .csv, .tsv, and .txt file formats
 - Concatenates multiple files, taking headers into account
 - Divides large files into output files of a specified size

### Usage
```
xcat [FILES] [OPTIONS]

Examples:
xcat input_1.csv input_2.csv --type=csv > combined_output_1.csv

xcat *.csv --type=csv --bytes=40K --threads --outdir=/home/ubuntu/workspace

Options:
--type=<type>                    Type of inputted files (csv,tsv,txt)
--bytes=<size>                   Divides inputted files into files of maximum <size> (B,K,M,G)
--threads                        Enable multithreading when --bytes option is used   
--outdir=<output_directory>      The directory where outputted files will be saved           
```
