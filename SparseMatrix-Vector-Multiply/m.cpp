#include <memory>
#include <sstream>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <cstdlib>
#include <iostream>
#include <string>
#include<omp.h>
#include <vector>



int main(int argc, const char* argv[]){
	if(argc<1)
		cout<<"enter file path"<<endl;
	int nnz=83663478;
	int max_column=1770961;
	int indexVector[nnz+1];
	string name="mat";
	vector<int> row_index;
	vector<int*> row_pointer;
	ifstream file(argv[1]);
        if(file.is_open()){
		cout<<string(argv[1])<<endl;
        	string line,token;
		int ix=0;
		int current_row=1;
		indexVector[0]=0;
		int *zero; 
		zero = indexVector;
		cout<<nnz<<endl;
		row_pointer.push_back(zero);
		row_index.push_back(*zero);
		int el;
		for(el=0;el<nnz;el++){	
			int i,j;int v_ij;
			file >> i >> j >> v_ij;
			if(i>current_row){
				row_index.push_back(i-1);
				current_row=i;
				indexVector[ix]=(j-1);
				int *tmp=&indexVector[ix];
				ix++;
				row_pointer.push_back(tmp);	
			}
			else{
				indexVector[ix]=j-1;
				ix++;
			}	
		}
		indexVector[ix]=0;
		row_pointer.push_back(&indexVector[nnz]);
		file.close();
                }else{
                   cout << "Failed to read file " << string(argv[0]) << endl;
                   return(0);
               }
	omp_set_num_threads(16);
	double x[max_column];
	double output[max_column];
	for(int i =0;i<max_column;i++)
		x[i] = (double)rand()/(double)RAND_MAX;
	double end =0;
	for(int p =0;p<10;p++){
	unsigned int k; int* iter;
	double start = omp_get_wtime();
	#pragma omp parallel for default(shared) private(k,iter)
	for(k=0;k<row_pointer.size()-1;k++){
		//#pragma omp parallel for default(shared) private(iter)
		for(iter=row_pointer[k];iter!=row_pointer[k+1];iter++){	
			output[row_index[k]]+=x[*iter];
		}
	}
	end += omp_get_wtime()-start;
	}
	cout<<end/10<<endl;
	return(0);
}
