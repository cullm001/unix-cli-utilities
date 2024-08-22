#include<stdio.h>
#include<string.h>
#include<stdbool.h>
#include <stdlib.h>
#define MAXCHAR 1000


int main(int argc, char *argv[]){
    
    if (argc == 1){
        printf("%s","Error: File argument not passed in");
        return 1;
    }
    
    char row[MAXCHAR];
    char header[1000];

    FILE* stream = fopen(argv[1], "r");
    if (stream == NULL){
        printf("Error: file %s NOT found\n", argv[1]);
        return 1;
    }
    fgets(header, MAXCHAR, stream);
    fclose(stream);

    int numCSV;
    if (strstr(argv[argc-2],"--bytes=") == NULL){
        numCSV = argc - 1;
    }
    else{
        numCSV = argc - 3;
    }

    for (int i = 2; i < numCSV; i++){
        FILE* stream = fopen(argv[i], "r");
        if (stream == NULL){
            printf("Error: file %s not found\n", argv[1]);
            return 1;
        }  
        fgets(row, MAXCHAR, stream);
        if (strcmp(header,row) != 0){
            printf("Error: %s contains different header schema\n", argv[i]);
            return 1;
        }
        fclose(stream);

    }

    if (strstr(argv[argc-2],"--bytes=") == NULL){
        printf("%s", header);
        for (int i = 1; i < argc; i++){
            FILE* stream = fopen(argv[i], "r");
            fgets(row, MAXCHAR, stream);

            while (true){
                fgets(row, MAXCHAR, stream);
                if (feof(stream)) break;
                printf("%s", row);
            }
            fclose(stream);
        }
    }

    else{
        char bytes[20];
        strcpy(bytes, argv[argc - 2]);

        char str_size[6];
        strncpy(str_size, bytes+8, strlen(bytes)-1 - 8 );
        long long fileCap = atoi(str_size);

        if (bytes[strlen(bytes)-1] == 'G' ){
            fileCap *= 1024 * 1024 * 1024;
        }
        else if(bytes[strlen(bytes)-1] == 'M') {
            fileCap *= 1024*1024;
        }
        else if(bytes[strlen(bytes)-1] == 'K') {
            fileCap *= 1024;
        }
        else if (bytes[strlen(bytes)-1] != 'B'){
            printf("%s","Error: incorrect file size format");
            return 1;
        }

        int currentSize = 0;
        int currentInputFile = 1;
        int currentOutputFile = 1;
        char fileName[20]; 

        FILE* ifstream = fopen(argv[1], "r");
        sprintf(fileName, "output_%d.csv", currentOutputFile);
        FILE* ofstream = fopen(fileName, "w");

        while (true){
            fgets(row, MAXCHAR, ifstream);
            int lineSize = strlen(row);

            if (feof(ifstream)){
                if (currentInputFile == numCSV)
                    break;
                currentInputFile++;
                fclose(ifstream);
                FILE* ifstream = fopen(argv[currentInputFile], "r");
                fgets(row, MAXCHAR, ifstream);

            }

            else if (lineSize + currentSize <= fileCap){
                currentSize+=lineSize;
            }

            else if (lineSize + currentSize > fileCap){
                fclose(ofstream);
                currentOutputFile++;
                sprintf(fileName, "output_%d.csv", currentOutputFile);
                FILE* ofstream = fopen(fileName, "w");
                currentSize = lineSize;
                fprintf(ofstream, "%s", row);

            }

        }        
    }
    
    return 0;
}