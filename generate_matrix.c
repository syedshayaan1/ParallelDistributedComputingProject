#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define MATRIX_SIZE 16

int main() {
    int matrix[MATRIX_SIZE][MATRIX_SIZE];
    int i, j;

    // Seed the random number generator
    srand(time(NULL));

    // Fill the matrix with random numbers
    for (i = 0; i < MATRIX_SIZE; i++) {
        for (j = 0; j < MATRIX_SIZE; j++) {
            matrix[i][j] = rand() % 10; // Generate a random number between 0 and 99
        }
    }

    // Open the file for writing
    FILE *f = fopen("matrix2.txt", "w");
    if (f == NULL) {
        printf("Error opening file!\n");
        exit(1);
    }

    // Write the matrix to the file
    for (i = 0; i < MATRIX_SIZE; i++) {
        for (j = 0; j < MATRIX_SIZE; j++) {
            fprintf(f, "%d ", matrix[i][j]);
        }
        fprintf(f, "\n");
    }

    // Close the file
    fclose(f);

    return 0;
}