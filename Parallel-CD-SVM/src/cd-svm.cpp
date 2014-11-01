                #include <fstream>
                #include <string>
                #include <iostream>
                #include <Eigen/Dense>
                #include <Eigen/Sparse>
                #include <vector>
                #include <set>
                #include <math.h>
                #include <Eigen/Core>
                #include<omp.h>

		#include <algorithm>
                using namespace Eigen;
                using namespace std;

                typedef Eigen::Triplet<double> T;
                    int load_data(string file_name,VectorXi&, SparseMatrix<double>&);
                    VectorXi rpermute(int n); 

                int main(int argc, const char* argv[]){
        	    if(argc < 5)
            		cout << "Atleast 5 arguments" <<endl;
            	    double C = atof(argv[1]);
            	    int nr_threads = atoi(argv[2]);
            	    const char* data = argv[3];
            	    const char* test_data = argv[4];
                    SparseMatrix<double> X,X_t;
                    VectorXi Y,Y_t;
                    string file_name = string(data);
                    string test_file_name = string(test_data);
                    cout<<file_name<<endl;
		    cout<<test_file_name<<endl;
		    load_data(file_name,Y,X);
		    load_data(test_file_name,Y_t,X_t);
                    VectorXd alpha = VectorXd::Zero(Y.size());
		    VectorXd W = VectorXd::Zero(Y.size());
		    double cum = 0.0;
		    VectorXd e = VectorXd::Ones(Y.size());
                    for(unsigned int iter =0; iter<20;iter++){
			VectorXd new_W = VectorXd::Zero(Y.size());
                        omp_set_num_threads(nr_threads);
                        VectorXi pi = rpermute(Y.size());
			int s,i;
			double delta,temp1,temp2;
                        double start = omp_get_wtime();
		     #pragma omp parallel for default(shared) private(i,delta,temp1,temp2)
                        for(s = 0; s <Y.size(); s++){
                            i=pi(s);
			    temp1 =0.0;
			    temp2 = 0.0;
			    for (SparseMatrix<double>::InnerIterator it(X,i); it; ++it){
				temp1 = temp1 + (it.value()*it.value());
				temp2 = temp2 + (it.value()*W(it.row()));	
			    }
			delta = (1-(Y(i)*temp2)-(alpha(i)/(2*C)))/((Y(i)*Y(i)*temp1)+(1/(2*C)));
			delta=max(delta,-alpha(i));
			alpha(i) = alpha(i)+delta;
			#pragma omp critical
                        {
			 for (SparseMatrix<double>::InnerIterator it(X,i); it; ++it){
				W(it.row()) = W(it.row())+ (Y(i)*delta*it.value());
			      }
			}
			}
			double end = omp_get_wtime()-start;
			double f = 0.5*W.squaredNorm()+((0.25/C)*alpha.squaredNorm())-(alpha.sum());
			VectorXd zi = VectorXd::Zero(Y.size());
			for(int k=0;k<Y.size();k++){
				double temp = 0.0;
				for (SparseMatrix<double>::InnerIterator it(X,k); it; ++it){
					temp = temp + W(it.row())*it.value();
				}
			    temp = temp*Y(k);
			    if((1-temp)>0.0)
				zi(k) = 1-temp;
			}
			double g = (0.5*W.squaredNorm())+(C*zi.squaredNorm());
			int correct_train=0;
			for (int i=0; i<X.outerSize(); ++i){
				double sum_t=0.0;
				for (SparseMatrix<double>::InnerIterator oit(X,i); oit; ++oit){
				  new_W(oit.row()) =new_W(oit.row()) + (Y(i)*alpha(i)*oit.value());	
				  sum_t = sum_t + oit.value()*W(oit.row());
				}
				if(sum_t>0.0&& Y(i)>0)
                                        correct_train++;
                                else if(sum_t<0.0&& Y(i)<0)
                                        correct_train++;
			}
			int correct = 0;	
			for (int i=0; i<X_t.outerSize(); ++i){
				double sum =0.0;
                                for (SparseMatrix<double>::InnerIterator it(X_t,i); it; ++it){
                                	sum =sum+it.value()*W(it.row());
				}
				if(sum>0.0&& Y_t(i)>0)
                                        correct++;
                                else if(sum<0.0&& Y_t(i)<0)
                                        correct++;
			}
			//cout<<end<<"\t"<<(g-33346.9)/33346.9<<endl;
			double accuracy = (double)correct/Y_t.size();
			cum+=end;
            		cout << "iter "<<iter+1 <<" walltime "<<end<<" f(alpha) = "<<f<<" g(w) = "<<g<<" norm is "<<(new_W-W).squaredNorm()<<" Test accuracy = "<<accuracy<<" Train accuracy = "<<(double)correct_train/Y.size()<<endl;
                    }
			cout<<"cumulative time taken for 20 iterations is: "<<cum<<endl;
			return(0);
            }

            int load_data(string file_name, VectorXi& Y,SparseMatrix<double>& X){
                    vector<T> tripletList;
		    int i;
                    double v_ij;
                    char c;
		    unsigned int row=0;
                    unsigned int j;
		    unsigned int max_f =0;
                    ifstream file(file_name);
		    if(file.is_open()){
			string line;
                    while(getline(file, line)){
                           stringstream tmp(line);    
			    tmp>> i;
			    Y.conservativeResize(row+1);
                            Y(row)= i;
                            while(tmp >> j >> c >> v_ij){
                                if(j>max_f)
				 	max_f =j;
				tripletList.push_back(T(j,row,v_ij));
			    }
				row++;
                        }
                        X.resize(max_f+1,row+1);
			X.setFromTriplets(tripletList.begin(), tripletList.end());
                        file.close();
                    }else{
                        cout << "Failed to read file " << file_name << endl;
                        return(0);
                    }
                    
                return(1);
	}
                
    VectorXi rpermute(int n) {
    	VectorXi a;
	a.resize(n);
    	int k;
    	for (k = 0; k < n; k++)
		a(k) = k;
    	for (k = n-1; k > 0; k--) {
		int j = rand() % (k+1);
		int temp = a(j);
		a(j) = a(k);
		a(k) = temp;
    	}
    	return a;
	}
