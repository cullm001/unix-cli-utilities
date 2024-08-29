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
    char** inputFiles;
    char* outputDir;
    int numFiles;
    long long *fileSizes;
    char* header;
    long long fileCap;
    long long startingByte;
    char fileType[4];
    int isSVFile;
};

int main(int argc, char *argv[]) {
    
    if (argc == 1) {
        printf("Error: No arguments passed in\n");
        return 1;
    }
    
    int numFiles = 0;
    for (int i = 1; i < argc; i++) {
        if (strstr(argv[i], "--type=") == NULL &&
            strstr(argv[i], "--bytes=") == NULL &&
            strstr(argv[i], "--threads") == NULL &&
            strstr(argv[i], "--outdir=") == NULL) {
            numFiles++;
        }
    }
    if (numFiles == 0){
        printf("Error: No files passed in\n");
        return 1;
    }
    char bytes[10] = "";
    char outdir[30] = "./";
    int threads = 0;
    long long fileCap;
    char *files[numFiles];
    int isSVFile = 1;
    char fileType[4] = "csv";

    int fileCounter = 0;
    for (int i = 1; i < argc; i++) {
        if (strstr(argv[i], "--bytes=") != NULL) {
            strncpy(bytes, argv[i] + 8, strlen(argv[i]) - 8);
            bytes[strlen(argv[i]) - 8] = '\0';
            fileCap = atoll(bytes);

            if (bytes[strlen(bytes) - 1] == 'G') {
                fileCap *= 1000 * 1000 * 1000;
            } else if (bytes[strlen(bytes) - 1] == 'M') {
                fileCap *= 1000 * 1000;
            } else if (bytes[strlen(bytes) - 1] == 'K') {
                fileCap *= 1000;
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
            struct stat info;
            if (stat(outdir, &info) != 0){
                printf("Error: output directory does not exist");
                return 1;
            }
        } 
        else if (strstr(argv[i], "--type=") != NULL){
            if (strcmp(argv[i] + 8, "sv") != 0) {
                isSVFile = 0;
            }
            strcpy(fileType, argv[i] + 7);
        }
        else { 
            files[fileCounter] = malloc(30 * sizeof(char)); 
            strcpy(files[fileCounter], argv[i]);
            fileCounter++;
        }
    }



    char row[MAXCHAR];
    char header[MAXCHAR];
    
    FILE *stream;
    for (int i = 0; i < numFiles; i++) {
        stream = fopen(files[i], "r");
        if (stream == NULL) {
            printf("Error: file %s not found\n", files[i]);
            return 1;
        }
        if (isSVFile){
            fgets(row, MAXCHAR, stream);
            if (i == 0){
                strcpy(header,row);
            }
            else if (strcmp(header, row) != 0) {
                printf("Error: %s contains a different header schema\n", argv[i]);
                return 1;
            }
        }
        fclose(stream);
    }
   

    if (strlen(bytes) == 0) {
        if (isSVFile)  
            printf("%s", header);
        for (int i = 0; i < numFiles; i++) {
            stream = fopen(files[i], "r");
            if (isSVFile)
                fgets(row, MAXCHAR, stream); 
            while (fgets(row, MAXCHAR, stream) != NULL) {
                printf("%s", row);
            }
            fclose(stream);
        }

    } else if (threads == 0) {


        
        int currentSize = 0;
        int currentInputFile = 0;
        int currentOutputFile = 1;
        char fileName[100];
        if (isSVFile)
            fileCap -= strlen(header);

        FILE *ifstream = fopen(files[currentInputFile], "r");
        if (ifstream == NULL) {
            printf("Error: file %s not found\n", files[currentInputFile]);
            return 1;
        }
        sprintf(fileName, "%s/output_%d.%s", outdir, currentOutputFile, fileType);
        FILE *ofstream = fopen(fileName, "w");

        if (isSVFile){
            fprintf(ofstream, "%s", header); 
            fgets(row, MAXCHAR, ifstream);  
        }
        while (true) {
            if (fgets(row, MAXCHAR, ifstream) == NULL) {
                fclose(ifstream);
                currentInputFile++;
                if (currentInputFile == numFiles) {
                    break;
                }

                ifstream = fopen(files[currentInputFile], "r");
                if (ifstream == NULL) {
                    printf("Error: file %s not found\n", files[currentInputFile]);
                    break;
                }
                if (isSVFile)
                    fgets(row, MAXCHAR, ifstream);  
                continue; //check this
            }

            int lineSize = strlen(row);
            if (lineSize + currentSize <= fileCap) {
                fprintf(ofstream, "%s", row);
                currentSize += lineSize;
            } else {
                fclose(ofstream);
                currentOutputFile++;
                sprintf(fileName, "%s/output_%d.%s", outdir, currentOutputFile, fileType);
                ofstream = fopen(fileName, "w");
                if (isSVFile)
                    fprintf(ofstream, "%s", header); 
                fprintf(ofstream, "%s", row);
                currentSize = lineSize;
            }
        }

        fclose(ofstream);
    }

    else if (threads == 1){
        long long totalSize = 0;
        long long* fileSizes = malloc(numFiles * sizeof(long long));
        FILE* fp;
        struct stat st;
        for (int i = 0; i < numFiles; i++){
            stat(files[i], &st);
            totalSize += (st.st_size-strlen(header));
            fileSizes[i] = st.st_size;
        }

        
        if (isSVFile)
            fileCap -= strlen(header);
        int numThreads = 0;
        int currSize = 0;
        long long *startingBytes = malloc(numFiles * sizeof(long)); 
        bool startingThread = false;
        for (int i = 0; i < numFiles; i++) {
            stream = fopen(files[i], "r");
            bool newFile = true;
            if (isSVFile)
                fgets(row, MAXCHAR, stream); 

            while (fgets(row, MAXCHAR, stream) != NULL) {
                if (currSize + strlen(row) > fileCap){
                    if (startingThread){
                        startingThread = false;
                        startingBytes[numThreads]=strlen(header);
                    }
                    else if (newFile){
                        if (isSVFile)
                            currSize += strlen(header);
                        newFile = false;
                    }
                    numThreads++;
                    startingBytes[numThreads] = currSize + startingBytes[numThreads-1];
                    currSize = 0;
                }
                currSize+= strlen(row);

            }
            fclose(stream);
        }
        numThreads++;

        pthread_t threads[numThreads];
        struct threadInfo threadInfos[numThreads];
        for (int i = 0; i < numThreads; i++){
            
            threadInfos[i].threadNum = i;
            threadInfos[i].outputDir = outdir;
            threadInfos[i].startingByte = startingBytes[i];
            threadInfos[i].inputFiles = files;
            threadInfos[i].numFiles = numFiles;
            threadInfos[i].fileSizes = fileSizes;
            if (isSVFile)
                threadInfos[i].header = header;
            threadInfos[i].fileCap=fileCap;
            strcpy(threadInfos[i].fileType, fileType);
            threadInfos[i].isSVFile = isSVFile;


            pthread_create(&threads[i], NULL, fileWrite, (void*)&threadInfos[i]);
        }

        for (int i = 0; i < numThreads ; i++) {
            pthread_join(threads[i], NULL);
        }

        free(startingBytes);
    }
    
    return 0;
}


void* fileWrite(void* arg) {
    struct threadInfo* info = (struct threadInfo*) arg;
    char outputFile[50];
    sprintf(outputFile, "%s/output_%d.%s", info->outputDir, info->threadNum+1, info->fileType);
    int currentFileIndex = 0;
    long long currentByte = info->startingByte;
    while (currentFileIndex < info->numFiles && currentByte >= info->fileSizes[currentFileIndex]) {
        currentByte -= info->fileSizes[currentFileIndex];
        currentFileIndex++;
    }
    FILE *ofstream = fopen(outputFile, "w");

    if (info->isSVFile)
        fprintf(ofstream, "%s", info->header);

    FILE* ifstream;
    char row[MAXCHAR];
    long long currentSize = 0;

    for (int i = currentFileIndex; i < info->numFiles; i++){
        
        ifstream=fopen(info->inputFiles[i],"r");
        if (ifstream == NULL) {
            return NULL;
        }

        if (currentSize == 0)
          fseek(ifstream, currentByte, SEEK_SET);
        
        while (true){
            if (fgets(row, MAXCHAR, ifstream) == NULL){
                fclose(ifstream);
                break;
            
            }
            if (strlen(row) + currentSize > info->fileCap){
                fclose(ifstream);
                fclose(ofstream);
                return NULL;
            }

            if (!info->isSVFile || strstr(row, info->header) == NULL){
                fprintf(ofstream, "%s", row);
                currentSize += strlen(row);
            }
        }
        
    }
    fclose(ofstream);

    return NULL;
}
