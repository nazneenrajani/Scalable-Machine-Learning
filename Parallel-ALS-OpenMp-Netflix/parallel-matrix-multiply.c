#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#define M 500
#define N 500

int main(int argc, char **argv) {
	int i, j, k,num,condition, threads;
	double **A, **B, **C;
	double sum;
	omp_set_num_threads(16);
	condition=1;

	A = malloc(M*sizeof(double *));
	B = malloc(M*sizeof(double *));
	C = malloc(M*sizeof(double *));
	for (i = 0; i < M; i++) {
		A[i] = malloc(N*sizeof(double));
		B[i] = malloc(N*sizeof(double));
		C[i] = malloc(N*sizeof(double));
	}
	for (i = 0; i < M; i++) {
		for (j = 0; j < N; j++) {
			A[i][j] = i+j;
			B[i][j] = i*j;
			C[i][j] = 0;
		}
	}
	printf("Max number of threads: %i \n",omp_get_max_threads());
	for(condition=0;condition<4;condition++){
	double avg_time=0.0;
	for(num=0;num<10;num++){
		double start = omp_get_wtime();
		switch(condition){
			case 0:
				for (i = 0; i < M; i++) {
					for (j = 0; j < N; j++) {
						sum = 0;
						for (k=0; k < M; k++) {
							sum += A[i][k]*B[k][j];
						}
						C[i][j] = sum;
					}
				}
				break;
			case 1:
			#pragma omp parallel default(shared) private(i,j,k,sum)
				{
			#pragma omp for
				for(i=0;i<M;i++) {
					for(j=0;j<N;j++) {
						sum = 0;
						for(k=0;k<M;k++) {
							sum += A[i][k]*B[k][j];
						}
							C[i][j] = sum;
						}
					}
				}
				break;
			case 2:
			for(i=0;i<M;i++) {
			#pragma omp parallel for default(shared) private(j,k,sum)
					for(j=0;j<N;j++) {
						sum = 0;
						for(k=0;k<M;k++) {
							sum += A[i][k]*B[k][j];
						}
						C[i][j] = sum;
					}
			}
				break;
			case 3:
			#pragma omp parallel for default(shared) private(i,j,k,sum)
				for(i=0;i<M;i++) {
					#pragma omp parallel for default(shared) private(j,k,sum)
					for(j=0;j<N;j++) {
						sum = 0;
						for(k=0;k<M;k++) {
							sum += A[i][k]*B[k][j];
							}
							C[i][j] = sum;
						}
				}
				break;
			}
		double end = omp_get_wtime();
		avg_time += (end-start);
	}
		printf("Average Time over 10 runs for %d threads: \t %f \n", omp_get_max_threads(),avg_time/10);
	}
}

double parallelprob1(double** A, double** B, double** C,int threads,int condition) {
	int i, j, k;
	double sum;
	printf("Max number of threads: %i \n",omp_get_max_threads());
	switch(condition){
	case 0:
		for (i = 0; i < M; i++) {
			for (j = 0; j < N; j++) {
				sum = 0;
				for (k=0; k < M; k++) {
					sum += A[i][k]*B[k][j];
				}
				C[i][j] = sum;
			}
		}
		break;
	case 1:
	#pragma omp parallel num_threads(threads) default(shared) private(i,j,k,sum)
		printf("Number of threads: %i \n",omp_get_num_threads());
	#pragma omp parallel
		for(i=0;i<M;i++) {
			for(j=0;j<N;j++) {
				sum = 0;
				for(k=0;k<M;k++) {
					sum += A[i][k]*B[k][j];
				}
				C[i][j] = sum;
			}
		}
		break;
	case 2:
		for(i=0;i<M;i++) {
	#pragma omp parallel num_threads(threads) default(shared) private(i,j,k,sum)
		printf("Number of threads: %i \n",omp_get_num_threads());
	#pragma omp parallel
			for(j=0;j<N;j++) {
				sum = 0;
				for(k=0;k<M;k++) {
					sum += A[i][k]*B[k][j];
				}
				C[i][j] = sum;
			}
		}
		break;
	case 3:
	#pragma omp parallel num_threads(threads) default(shared) private(i,j,k,sum)
		printf("Number of threads: %i \n",omp_get_num_threads());
		for(i=0;i<M;i++) {
	#pragma omp parallel num_threads(threads) default(shared) private(j,k,sum)
			for(j=0;j<N;j++) {
				sum = 0;
				for(k=0;k<M;k++) {
					sum += A[i][k]*B[k][j];
				}
				C[i][j] = sum;
			}
		}
		break;
	}
	return omp_get_wtime();
}

