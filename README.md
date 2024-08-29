# Unix CLI Utilities
A collection of command line utilities designed to simplify data processing tasks in Unix environments

## xcat
xcat extends the functionality of the standard Linux cat command by including extra support with handling CSV and TSV files. 
### Features
 - Supports .csv, .tvt, and .txt file formats
 - Concatenates multiple files, taking headers into account
 - Divides large files into output files of a specified size

### Usage
```
xcat [FILES] [OPTIONS]

Example:
xcat *.csv --type=csv --bytes=40K --threads --outdir=/home/ubuntu/workspace

Options:
--type=<type>                    Type of inputted files (csv,tsv,txt)
--bytes=<size>                   Divides the inputted files into output files of maximum <size> (B,K,M,G)
--threads                        Enable multithreading when --bytes option is used   
--outdir=<output_directory>      The directory where outputted files will be saved           
```
