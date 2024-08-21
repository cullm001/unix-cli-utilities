#include <stdio.h>
#include<string.h>
#include<stdbool.h>
#define MAXCHAR 1000

int main(int argc, char *argv[]){

    if (argc == 1){
        printf("Error: File argument not passed in\n");
        return 1;
    }

    char row[MAXCHAR];
    FILE* stream = fopen(argv[1], "r");

    if (stream == NULL){
        printf("Error: file %s not found\n", argv[1]);
        return 1;
    }

    char header[1000];
    fgets(header, MAXCHAR, stream);
    printf("%s", header);
    while (feof(stream) != true){
        fgets(row, MAXCHAR, stream);
        printf("Row: %s", row);
    }

    for (int i = 2; i < argc; i++){
        FILE* stream = fopen(argv[i], "r");
        fgets(row, MAXCHAR, stream);
        if (strcmp(header,row) != 0){
            printf("Error: %s contain different header schema\n", argv[i]);
            return 1;
        }
        while (feof(stream) != true){
            fgets(row, MAXCHAR, stream);
            printf("Row: %s", row);

        }
    }
    fclose(stream);


    return 0;
}