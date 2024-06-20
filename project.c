//20i-0436 Shayan Faisal
//20i-0871 Naik Ur Rehman
//20i-0647 Syed Shayaan Hasnain
//PDC Project

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define N 16 //size of matrices (2^4)

//key value pair struct
struct KeyValue 
{
    int *key;
    int value;
};

//function to read a matrix from file
void readMatrixFromFile(char* filename, int* matrix)
{
	FILE *fp;
	fp = fopen(filename, "r");
	for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < N; j++)
		{
			fscanf(fp, "%d", &matrix[i * N + j]);
		}
	}
	fclose(fp);
}

//function to write a matrix to file
void writeMatrixToFile(char* filename, int** matrix)
{
	FILE *fp;
	fp = fopen(filename, "w");
	for (int i = 0; i < N; i++)
	{
        for (int j = 0; j < N; j++)
        {
            fprintf(fp, "%d ", matrix[i][j]);
        }
        fprintf(fp, "\n");
    }
	fclose(fp);
}

//function to read key value pairs from file
void readKeyValuePairsFromFile(char* filename, struct KeyValue *pairs)
{
	FILE *fp;
	fp = fopen(filename, "r");
	for (int i = 0; i < N * N; i++)
	{
		int k1, k2, v;
		fscanf(fp, "%d, %d, %d", &k1, &k2, &v);
        int* key = (int*) malloc(2 * sizeof(int));
        key[0] = k1;
        key[1] = k2;
        pairs[i].key = key;
        pairs[i].value = v;
	}
	fclose(fp);
}

//function to write key value pairs to file
void writeKeyValuePairsToFile(char* filename, struct KeyValue *pairs, int start, int end)
{
	FILE *fp;
	fp = fopen(filename, "w");
    for (int i = start; i < end; i++)
    {
        for (int j = 0; j < N; j++)
        {
			fprintf(fp, "%d, %d, %d", pairs[i * N + j].key[0], pairs[i * N + j].key[1], pairs[i * N + j].value);
			fprintf(fp, "\n");
        }
    }
    fclose(fp);
}

//function for mapping
void map(int *matrix, struct KeyValue *pairs, int start, int end)
{
    for (int i = start; i < end; i++)
    {
        for (int j = 0; j < N; j++)
        {
            pairs[i * N + j].key = (int *) malloc(2 * sizeof(int));
            pairs[i * N + j].key[0] = i;
            pairs[i * N + j].key[1] = j;
            pairs[i * N + j].value = matrix[i * N + j];
        }
    }
}

//function for reducing
void reduce(struct KeyValue *pairsA, struct KeyValue *pairsB, struct KeyValue *pairsC)
{
    for (int i = 0; i < N; i++)
    {
        for (int j = 0; j < N; j++)
        {
            int sum = 0;
            for (int k = 0; k < N; k++)
            {
                sum += pairsA[i * N + k].value * pairsB[k * N + j].value;
            }
            
            pairsC[i * N + j].key = (int *) malloc(2 * sizeof(int));
            pairsC[i * N + j].key[0] = i;
            pairsC[i * N + j].key[1] = j;
            pairsC[i * N + j].value = sum;
        }
    }
}

//function for printing key value pairs
void printKeyValuePairs(struct KeyValue *pairs)
{
	for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < N; j++)
		{
			printf("%d, %d, %d", pairs[i * N + j].key[0], pairs[i * N + j].key[1], pairs[i * N + j].value);
			printf("\n");
		}
	}
}

//function to compare and check if two matrices are the same
int matrixComparisionFunction(int** matrix1, int** matrix2)
{
	for (int i = 0; i < N; ++i)
	{
  		for (int j = 0; j < N; ++j)
  		{
     		if (matrix1[i][j] != matrix2[i][j])
     		{
     			return 0;
     		}
  		}
	}
	return 1;
}

int main(int argc, char** argv)
{
    int rank, size, i, j, k, sum, len;
    char name[MPI_MAX_PROCESSOR_NAME];
    char *filenameA = argv[1];
    char *filenameB = argv[2];
    int signal = 0;
    
    //initializing data
    int *matrixA = (int *) malloc(N * N * sizeof(int));
	int *matrixB = (int *) malloc(N * N * sizeof(int));
	int **result = (int **) malloc(N * sizeof(int *));
	for (i = 0; i < N; i++)
	{
	    result[i] = (int *) malloc(N * sizeof(int));
	}
    struct KeyValue *pairsA = (struct KeyValue *) malloc(N * N * sizeof(struct KeyValue));
	struct KeyValue *pairsB = (struct KeyValue *) malloc(N * N * sizeof(struct KeyValue)); 
	struct KeyValue *pairsC = (struct KeyValue *) malloc(N * N * sizeof(struct KeyValue));
    
    //MPI initialization
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name(name, &len);
    MPI_Status status;
    
	//master process
    if (rank == 0)
    {
        //available slaves
        int slaves = size - 1;
        //printf("\nAvailable slaves: %d\n", slaves);
        int mappers = slaves - 1;
        int reducers = slaves - mappers;
        //printf("Mappers: %d\n", mappers);
        //printf("Reducers: %d\n", reducers);
		
		int rows = N / mappers;
		
		printf("\nMaster with process_id %d running on %s\n", rank, name);
		
		//assigning task of splitting the input to mappers
		for (i=0; i<mappers; i++)
		{
			signal = 1;
			printf("Task Map assigned to process %d\n", i+1);
			MPI_Send(&signal, 1, MPI_INT, i+1, 0, MPI_COMM_WORLD);
			
			//telling mapper which rows to map
			int start = i * rows;
			int end = start + rows;
			if (i == mappers-1)
			{
				end += N % mappers;
			}
			MPI_Send(&start, 1, MPI_INT, i+1, 0, MPI_COMM_WORLD);
			MPI_Send(&end, 1, MPI_INT, i+1, 0, MPI_COMM_WORLD);
		}

		//wait for signal from mappers
		for (i=0; i<mappers; i++)
		{
			MPI_Recv(&signal, 1, MPI_INT, i+1, 0, MPI_COMM_WORLD, &status);
			printf("Process %d has completed task Map\n", i+1);
		}
		
		//shuffling
		FILE* outA = fopen("kvA.txt", "w");
		FILE* outB = fopen("kvB.txt", "w");
		for (i=0; i<mappers; i++)
		{
			char file_name[20];
			sprintf(file_name, "kvA%d.txt", i);
			FILE* fp = fopen(file_name, "r");
			
			struct KeyValue *pairs = (struct KeyValue *) malloc(N * N * sizeof(struct KeyValue));
			int k1, k2, v;
			j = 0;
			while (fscanf(fp, "%d, %d, %d", &k1, &k2, &v) == 3) 
			{
				struct KeyValue kv;
				int* key = (int*) malloc(2 * sizeof(int));
				key[0] = k1;
				key[1] = k2;
				kv.key = key;
				kv.value = v;
				pairs[j] = kv;
				j++;
				fprintf(outA, "%d, %d, %d\n", k1, k2, v);
				//printf("%d, %d, %d\n", k1, k2, v);
			}
			
			char file_nameB[20];
			sprintf(file_nameB, "kvB%d.txt", i);
			fp = fopen(file_nameB, "r");
			
			struct KeyValue *pairsB = (struct KeyValue *) malloc(N * N * sizeof(struct KeyValue));
			j = 0;
			while (fscanf(fp, "%d, %d, %d", &k1, &k2, &v) == 3) 
			{
				struct KeyValue kv;
				int* key = (int*) malloc(2 * sizeof(int));
				key[0] = k1;
				key[1] = k2;
				kv.key = key;
				kv.value = v;
				pairs[j] = kv;
				j++;
				fprintf(outB, "%d, %d, %d\n", k1, k2, v);
				//printf("%d, %d, %d\n", k1, k2, v);
			}
			fclose(fp);
		}
		fclose(outA);
		fclose(outB);
		
		//assigning reduce jobs to reducers	
		for (i=mappers; i<slaves; i++)
		{
			signal = 2;
			printf("Task Reduce assigned to process %d\n", i+1);
			MPI_Send(&signal, 1, MPI_INT, i+1, 0, MPI_COMM_WORLD);
		}	
		
		//wait for signal from reducers
		for (i=mappers; i<slaves; i++)
		{
			MPI_Recv(&signal, 1, MPI_INT, i+1, 0, MPI_COMM_WORLD, &status);
			printf("Process %d has completed task Reduce\n", i+1);
		}	

		//reading key value pairs from file
		readKeyValuePairsFromFile("kvC.txt", pairsC);
		
		//converting key value pairs to matrix form
		for (i = 0; i < N; i++)
		{
		    for (j = 0; j < N; j++)
		    {
				result[i][j] = pairsC[i * N + j].value;
		    }
		}
		
		//write out results to file
		writeMatrixToFile("result.txt", result);
		
		printf("Job has been completed!\n");
		
		//print the result
		/*printf("\nResult of matrix multiplication:\n");
		for (int i = 0; i < N; i++)
		{
		    for (int j = 0; j < N; j++)
		    {
		        printf("%d ", result[i][j]);
		    }
		    printf("\n");
		}*/
		
		
		//performing serial matrix multiplication
		int **serialResult = (int **) malloc(N * sizeof(int *));
		for (i = 0; i < N; i++)
		{
			serialResult[i] = (int *) malloc(N * sizeof(int));
		}
		//reading matrices from file
		readMatrixFromFile(filenameA, matrixA);
		readMatrixFromFile(filenameB, matrixB);
		for (i = 0; i < N; ++i)
		{
      		for (j = 0; j < N; ++j)
      		{
      			serialResult[i][j] = 0;
         		for (k = 0; k < N; ++k)
         		{
            		serialResult[i][j] += matrixA[i * N + k] * matrixB[k * N + j];
         		}
      		}
   		}
   		
   		//comparing results
   		printf("Matrix comparision function returned: ");
   		if (matrixComparisionFunction(result, serialResult))
   		{
   			printf("True\n");	
   		}
   		else
   		{
			printf("False\n");
		}
   		
    }
    //slave processes
    else
    {
		//wait for signal from master
		MPI_Recv(&signal, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
		
		//mappers
		if (signal == 1)
		{
			printf("Process %d received task Map on %s\n", rank, name);	
			
			//wait for start and end indexes from master
			int start, end;
			MPI_Recv(&start, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
			MPI_Recv(&end, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
			//printf("Process %d start: %d\n", rank, start);	
			//printf("Process %d end: %d\n", rank, end);	
			
			//reading matrices from file
			readMatrixFromFile(filenameA, matrixA);
			readMatrixFromFile(filenameB, matrixB);
		
			//apply mapping (splitting input into key value pairs)
			map(matrixA, pairsA, start, end);
			map(matrixB, pairsB, start, end);
			
			//write key value pairs to file
			char filenameA[20];
			char filenameB[20];
			sprintf(filenameA, "kvA%d.txt", rank-1);
			sprintf(filenameB, "kvB%d.txt", rank-1);
			
			writeKeyValuePairsToFile(filenameA, pairsA, start, end);
			writeKeyValuePairsToFile(filenameB, pairsB, start, end);
			
			//inform master
			MPI_Send(&signal, 1, MPI_INT, 0, 0, MPI_COMM_WORLD); 
		}
		//reducers
		else if (signal == 2)
		{
			printf("Process %d received task Reduce on %s\n", rank, name);	
		
			//reading key value pairs from file	
			readKeyValuePairsFromFile("kvA.txt", pairsA);
			readKeyValuePairsFromFile("kvB.txt", pairsB);
				
			//apply reduction (matrix multiplication)	
			reduce(pairsA, pairsB, pairsC);
			
			//writing key value pairs to file
			writeKeyValuePairsToFile("kvC.txt", pairsC, 0, N);
			
			//inform master
		    MPI_Send(&signal, 1, MPI_INT, 0, 0, MPI_COMM_WORLD); 
		}
    }

    MPI_Finalize();
    return 0;
}