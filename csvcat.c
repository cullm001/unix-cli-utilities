#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <pthread.h>
#define MAXCHAR 1000

void* fileWrite(void* vargp);

struct threadInfo{
    int threadNum;
    const char** inputFiles;
    char* outputDir;
    int numFiles;
    long long *fileSizes;
    char* header;
    long long fileCap;
    long long *startingBytes;
};

int main(int argc, char *argv[]) {
    if (argc == 1) {
        printf("Error: File argument not passed in\n");
        return 1;
    }

    int numCSV = 0;
    char bytes[10] = "";
    char outdir[30] = "./";
    int threads = 0;
    long long fileCap;
    const char *files[numCSV];

    for (int i = 1; i < argc; i++) {
        if (strstr(argv[i], "--bytes=") == NULL &&
            strstr(argv[i], "--threads") == NULL &&
            strstr(argv[i], "--outdir=") == NULL) {
            numCSV++;
        }
    }

    

    for (int i = 1; i < argc; i++) {
        if (strstr(argv[i], "--bytes=") != NULL) {
            strncpy(bytes, argv[i] + 8, strlen(argv[i]) - 8);
            bytes[strlen(argv[i]) - 8] = '\0';
            fileCap = atoll(bytes);

            if (bytes[strlen(bytes) - 1] == 'G') {
                fileCap *= 1024 * 1024 * 1024;
            } else if (bytes[strlen(bytes) - 1] == 'M') {
                fileCap *= 1024 * 1024;
            } else if (bytes[strlen(bytes) - 1] == 'K') {
                fileCap *= 1024;
            } else if (bytes[strlen(bytes) - 1] != 'B') {
                printf("Error: incorrect file size format\n");
                return 1;
            }
        }
        else if (strstr(argv[i], "--threads") != NULL) {
            threads = 1;
        } 
        else if (strstr(argv[i], "--outdir=") != NULL) {
            strncpy(outdir, argv[i] + 9, strlen(argv[i]) - 9);
            outdir[strlen(argv[i]) - 9] = '\0';
        } 
        else {
            files[i-1] = argv[i];
        }
    }
    for (int i =0; i < numCSV; i++){
        printf("%s ", files[i]);
    }

    printf("Number of Inputted files: %d\n", numCSV);

    char row[MAXCHAR];
    char header[MAXCHAR];

    FILE *stream = fopen(argv[1], "r");
    if (stream == NULL) {
        printf("Error: file %s NOT found\n", argv[1]);
        return 1;
    }
    fgets(header, MAXCHAR, stream);
    fclose(stream);

    for (int i = 2; i <= numCSV; i++) {
        stream = fopen(argv[i], "r");
        if (stream == NULL) {
            printf("Error: file %s not found\n", argv[i]);
            return 1;
        }
        fgets(row, MAXCHAR, stream);
        if (strcmp(header, row) != 0) {
            printf("Error: %s contains a different header schema\n", argv[i]);
            return 1;
        }
        fclose(stream);
    }

    

    if (strlen(bytes) == 0) {  
        // printf("%s", header);
        printf("Line: %s Size: %lu\n", header, strlen(header));
        for (int i = 0; i < numCSV; i++) {
            stream = fopen(files[i], "r");
            fgets(row, MAXCHAR, stream); 
            while (fgets(row, MAXCHAR, stream) != NULL) {
                //printf("%s", row);
                //int lineSize = strlen(row);

                printf("Line: %s Size: %lu\n", row, strlen(row));
            }
            fclose(stream);
        }
    } else if (threads == 0) {
        printf("Break up into files\n");

        
        //check for directory
        int currentSize = 0;
        int currentInputFile = 1;
        int currentOutputFile = 1;
        char fileName[100];
        printf("File cap: %lli\n", fileCap);
        fileCap -= strlen(header);
        FILE *ifstream = fopen(argv[currentInputFile], "r");
        if (ifstream == NULL) {
            printf("Error: file %s not found\n", argv[currentInputFile]);
            return 1;
        }

        sprintf(fileName, "%s/output_%d.csv", outdir, currentOutputFile);
        FILE *ofstream = fopen(fileName, "w");

        fprintf(ofstream, "%s", header); 
        fgets(row, MAXCHAR, ifstream);  
        while (true) {
            if (fgets(row, MAXCHAR, ifstream) == NULL) {
                fclose(ifstream);
                currentInputFile++;
                if (currentInputFile > numCSV) {
                    break;
                }
                ifstream = fopen(argv[currentInputFile], "r");
                if (ifstream == NULL) {
                    printf("Error: file %s not found\n", argv[currentInputFile]);
                    break;
                }
                fgets(row, MAXCHAR, ifstream);  
                continue;
            }

            int lineSize = strlen(row);

            if (lineSize + currentSize <= fileCap) {
                fprintf(ofstream, "%s", row);
                currentSize += lineSize;
            } else {
                fclose(ofstream);
                currentOutputFile++;
                sprintf(fileName, "%s/output_%d.csv", outdir, currentOutputFile);
                ofstream = fopen(fileName, "w");
                fprintf(ofstream, "%s", header); 
                currentSize = lineSize;
                fprintf(ofstream, "%s", row);
            }
        }

        fclose(ofstream);
    }

    else if (threads == 1){
        // printf("HELLO\n");
        //total size of a1l files
        long long totalSize = 0;
        long long* fileSizes = malloc(numCSV * sizeof(long long));
        FILE* fp ;
        struct stat st;
        for (int i = 0; i < numCSV; i++){
            stat(files[i], &st);
            totalSize += (st.st_size-strlen(header));
            fileSizes[i] = st.st_size;
            printf("%lli\n", st.st_size);
        }


        
     
        fileCap -= strlen(header);
        int numThreads = 1;
        int currSize = 0; 
        long long *startingBytes = malloc(numCSV * sizeof(long));
        startingBytes[0] = strlen(header); 
        for (int i = 0; i < numCSV; i++) {
            stream = fopen(files[i], "r");
            fgets(row, MAXCHAR, stream); 
            while (fgets(row, MAXCHAR, stream) != NULL) {
                if (currSize + strlen(row) > fileCap){
                    startingBytes[numThreads] = currSize + startingBytes[numThreads-1];
                    numThreads++;
                    currSize = 0;
                }
                currSize+= strlen(row);

            }
            fclose(stream);
        }

        pthread_t threads[numThreads];
        struct threadInfo threadInfos[numThreads];
        for (int i = 0; i < numThreads; i++){
            
            threadInfos[i].threadNum = i;
            threadInfos[i].outputDir = outdir;
            threadInfos[i].startingBytes = startingBytes;
            threadInfos[i].inputFiles = files;
            threadInfos[i].numFiles = numCSV;
            threadInfos[i].fileSizes = fileSizes;
            threadInfos[i].header = header;
            threadInfos[i].fileCap=fileCap;

            pthread_create(&threads[i], NULL, fileWrite, (void*)&threadInfos[i]);
        }

        //printf("THreads created\n");
        for (int i = 0; i < numThreads; i++) {
            pthread_join(threads[i], NULL);
        }

        free(startingBytes);


        for (int i = 0; i < numThreads; i++) {
            pthread_join(threads[i], NULL);
        }
        
    }

    return 0;
}


void* fileWrite(void* arg) {
    struct threadInfo* info = (struct threadInfo*) arg;
    char outputFile[50];
    sprintf(outputFile, "%s/output_%d.csv", info->outputDir, info->threadNum+1);

    long long currentByte = info->startingBytes[info->threadNum];

    FILE *ofstream = fopen(outputFile, "w");
    fprintf(ofstream, "%s", info->header);


    FILE* ifstream;
    char row[MAXCHAR];
    long long currentSize = 0;

    for (int i = 0; i < info->numFiles; i++){
        
        ifstream=fopen(info->inputFiles[i],"r");
        if (ifstream == NULL) {
            printf("Error: file %s not found\n", info->inputFiles[i]);
            return NULL;
        }

        fseek(ifstream, currentByte, SEEK_SET);


        while (true){
            if (fgets(row, MAXCHAR, ifstream) == NULL){
                fclose(ifstream);
                break;
            }
            if (strlen(row) + currentSize > info->fileCap){
                fclose(ifstream);
                break;
            }
            int lineSize = strlen(row);

            if (strstr(row, info->header) == NULL){
                fprintf(ofstream, "%s", row);
                currentSize += strlen(row);
            }


            
        }
       
       

        
    }
    fclose(ofstream);

    return NULL;
}
