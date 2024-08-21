#include <stdio.h>
#include<string.h>
#include<stdbool.h>
#define MAXCHAR 1000

int main(int argc, char *argv[]){

    if (argc == 2){
        printf("One file argument\n");
        
        FILE* first_stream;
        char row[MAXCHAR];
        first_stream = fopen(argv[1],"r");
        while (feof(first_stream) != true){
            fgets(row, MAXCHAR, first_stream);
            printf("Row: %s", row);
        }

    }

    if (argc == 3){
        printf("Two file argument\n");

        FILE* first_stream;
        char row[MAXCHAR];
        first_stream = fopen(argv[1], "r");
        while (!feof(first_stream)){
            fgets(row, MAXCHAR, first_stream);
            printf("Row: %s", row);
        }

        FILE* second_stream;
        second_stream = fopen(argv[2], "r");
        fgets(row, MAXCHAR, second_stream);
        while (!feof(second_stream)){
            fgets(row, MAXCHAR, second_stream);
            printf("Row: %s", row);
        }
    }

    return 0;
}