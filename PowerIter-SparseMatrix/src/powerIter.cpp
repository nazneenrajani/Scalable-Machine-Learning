#include <math.h>
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
#include <algorithm>
using namespace std;
bool sortBefore(int i, int j);
int nnz=83663478;
int max_column=1770961;
double x[1770961];
int main(int argc, const char* argv[]){
	int nnz=83663478;
	int max_column=1770961;
	if(argc<1)
		cout<<"enter file path"<<endl;
	int indexVector[nnz+1];
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
	double output[max_column];
	for(int i =0;i<max_column;i++)
		x[i] = (double)1/sqrt(max_column);
	double out[max_column];
	double end =0;
	for(int t =0;t<50;t++){
		unsigned int k; int* iter;
		double start = omp_get_wtime();
		#pragma omp parallel for default(shared) private(k,iter)
		for(k=0;k<row_pointer.size()-1;k++){
			double put=0;
			for(iter=row_pointer[k];iter!=row_pointer[k+1];iter++){	
				put+=x[*iter];
			}
			output[row_index[k]]=put;
		}
		double norm =0.0;
                for(int i =0; i<max_column;i++){
                        x[i] = output[i];
			norm+=(x[i]*x[i]);
		}
                double r = sqrt(norm);
		for(int i =0;i<max_column;i++){
			double root = x[i]/r;
			x[i] = root;
		}
		end += omp_get_wtime()-start;
		}
		cout<<end<<" is the walltime"<<endl;
		unsigned int k; int* iter;
		#pragma omp parallel for default(shared) private(k,iter)
		for(k=0;k<row_pointer.size()-1;k++){
			double put=0;
                        for(iter=row_pointer[k];iter!=row_pointer[k+1];iter++){
                                put+=x[*iter];
                        }
			out[row_index[k]] = put;
                }
		double lambda =0.0;
                for(int i =0; i<max_column;i++)
                        lambda+=(out[i]*x[i]);
		cout<<lambda<<" is the value of converged Lambda"<<endl;
		vector<double> x_vector(x, x + sizeof x / sizeof x[0]);
		vector<int>idx(x_vector.size());
		for (unsigned int i = 0; i != idx.size(); ++i) idx[i] = i;
		sort(idx.begin(), idx.end(),sortBefore);
		cout<<"Rank \t Node Index \t Value"<<endl;
		for(int rank=0;rank<100;rank++)
			cout<<rank+1<<"\t"<<idx[rank]+1<<"\t"<<x[idx[rank]]<<endl;	
		return(0);
}
bool sortBefore( int i, int j) {
  return x[i]> x[j];
}
