                #include <fstream>
                #include <string>
                #include <iostream>
                #include <Eigen/Dense>
                #include <Eigen/Sparse>
                #include <igl/slice.h>
                #include <igl/slice_into.h>
                #include <igl/colon.h>
                #include <vector>
                #include <set>
                #include <math.h>
                #include <Eigen/Core>
                #include<omp.h>
                #include <stdio.h>
                #include <stdlib.h>

                using namespace Eigen;
                using namespace std;
                using namespace igl;

                typedef Eigen::Triplet<double> T;
                    int read_data(const char* data,int& k, double& l,int nr_threads);
                    int load_data(int nnz, string file_name,bool isTest, int u, int m);
                    VectorXd ridge(MatrixXd&, VectorXd&, double&,int&);
                    double kridge(VectorXd&);
                    double rmse(SparseMatrix<double>&, MatrixXd&, MatrixXd&);
                    MatrixXd U, M;
                    int User,Movies,k;
                    set<int> column_R;
                    set<int> row_R;
                    VectorXi row_index,column_index,inner_row,inner_column;
                    SparseMatrix<double> R,R_t,R_r;
			int read_data(const char* data,int& k, double& l,int nr_threads){
                    int train_nnz,test_nnz;
		   double lambda = l;
                    string train_file,test_file;
                    unsigned int row = 0;
                    ifstream file(string(data)+"meta");
                    if(file.is_open()){
                        string line,token;
                        while(getline(file, line)){
                            stringstream tmp(line);
                            unsigned int col = 0;
                            while(getline(tmp, token, ' ')){
                                if(row == 0){
                                    if(col ==0)
                                        User = atoi(token.c_str());
                                    else
                                        Movies = atoi(token.c_str());
                                }else if (row == 1){
                                    if(col == 0)
                                        train_nnz = atoi(token.c_str());
                                    else
                                        train_file = token.c_str();
                                }
                                else{
                                    if(col == 0)
                                        test_nnz = atoi(token.c_str());
                                    else
                                        test_file = token.c_str();
                                }
                                col++;
                            }
                            row++;
                        }
                        printf("U is %d\n",User);
                        printf("M is %d\n",Movies);
                        cout << train_file <<endl;
                        cout << test_file <<endl;
                        file.close();
                    }else{
                        cout << "Failed to read file " << data << endl;
                        return(0);
                    }
                    string file_name = string(data)+train_file;
                    string test_file_name = string(data)+test_file;
                    cout<< file_name << endl;
                    load_data(train_nnz,file_name,false,User,Movies);
			load_data(test_nnz,test_file_name,true,User,Movies);
		    U.resize(k,User);
                    M.resize(k,Movies);
                    for (unsigned int i=0; i<k; i++) {
                        for(unsigned int j =0;j<User;j++){
                            U(i,j) = (double)rand()/(double)RAND_MAX;
                        }
                    }
                    for (unsigned int i=0; i<k; i++) {
                        for(unsigned int j =0;j<Movies;j++){
                            M(i,j) = (double)rand()/(double)RAND_MAX;
                        }
                    }
                    for(unsigned int nnz_index=0;nnz_index<User;nnz_index++){
                        if(row_R.find(nnz_index)==row_R.end()){
                                U.col(nnz_index).setZero();
                        }
                            
                    }
                    for(unsigned int nnz_index=0;nnz_index<Movies;nnz_index++){
                        if(column_R.find(nnz_index)==column_R.end()){
                                M.col(nnz_index).setZero();
                        }
                        
                    }
		   double cum = 0.0;
                    for(unsigned int iter =0; iter<10;iter++){
                        omp_set_num_threads(nr_threads);
                        double start = omp_get_wtime();
                        int i,j;
            //VectorXi inner_row,inner_column;
            		VectorXd t,Y,Z;
            		MatrixXd X,Xu;
                        #pragma omp parallel for default(shared) private(i,row,Y,X,inner_row,t)
                        for(i = 0; i <column_index.size(); i++){
                            unsigned int row=0;
                            for (SparseMatrix<double>::InnerIterator it(R,column_index(i)); it; ++it)
                            {
                                Y.conservativeResize(row+1);
                		Y(row)=it.value();
                		inner_row.conservativeResize(row+1);
                                inner_row(row)=it.row();
                                row++;
                            }
                            slice(U,inner_row,2,X);
                            t = ridge(X,Y,lambda,k);
				M.col(column_index(i)) = t;
                        }
            #pragma omp parallel for default(shared) private(i,row,Z,Xu,inner_column,t)
                        for(i = 0; i <row_index.size(); i++){
                            unsigned int row=0;
                                for (SparseMatrix<double>::InnerIterator it(R_r,row_index(i)); it; ++it)
                                {
                                        Z.conservativeResize(row+1);
                                        Z(row)=it.value();
                                        inner_column.conservativeResize(row+1);
                                        inner_column(row)=it.row();
                                        row++;
                                }
                            slice(M,inner_column,2,Xu);
			    t = ridge(Xu,Z,lambda,k);
                            U.col(row_index(i)) = t;
                        }
                       double end = omp_get_wtime()-start;
			cum+=end;
                        cout << "iter "<<iter+1 <<" walltime "<<end<<" test rmse "<<sqrt(rmse(R_t,U,M)/test_nnz)<<" train rmse "<<sqrt(rmse(R,U,M)/train_nnz)<<endl;
                    }
			cout<<"cumulative time taken for 10 iterations is: "<<cum<<endl;
                    	cout<<" test rmse "<<sqrt(rmse(R_t,U,M)/test_nnz)<<" train rmse "<<sqrt(rmse(R,U,M)/train_nnz)<<endl;
			return(1);
                }

            int load_data(int nnz, string file_name,bool isTest,int u , int m){
                if(isTest){
                    vector<T> tripletList;
                    tripletList.reserve(nnz);
                    int i,j;
            double v_ij;
                    unsigned int row = 0;
                    R_t.resize(u,m);
                    ifstream file(file_name);
                    if(file.is_open()){
                        string line,token;
                        while(getline(file, line)){
                            stringstream tmp(line);
                            unsigned int col = 0;
                            while(getline(tmp, token, ' ')){
                                if(col == 0){
                                    i = atoi(token.c_str())-1;
                                }else if (col == 1){
                                    j = atoi(token.c_str())-1;
                                }
                                else{
                                    v_ij = atof(token.c_str());
                                }
                                col++;
                            }
                            tripletList.push_back(T(i,j,v_ij));
                            row++;
                        }
                        R_t.setFromTriplets(tripletList.begin(), tripletList.end());
                        file.close();
                    }else{
                        cout << "Failed to read file " << file_name << endl;
                        return(0);
                    }
                    
                }
                else{
                    vector<T> tripletList;
                    tripletList.reserve(nnz);
                    int i,j,cc=0,rc=0;
        double v_ij;
                    unsigned int row = 0;
                    R.resize(u,m);
                    ifstream file(file_name);
                    if(file.is_open()){
                        string line,token;
                        while(getline(file, line)){
                            stringstream tmp(line);
                            unsigned int col = 0;
                            while(getline(tmp, token, ' ')){
                                if(col == 0){
                                    i = atoi(token.c_str())-1;
                                    if(row_R.find(i)==row_R.end()){
                                        row_R.insert(i);
                                        row_index.conservativeResize(rc+1);
                                        row_index(rc)=i;
                                        rc++;
                                    }
                                }else if (col == 1){
                                    j = atoi(token.c_str())-1;
                                    if(column_R.find(j)==column_R.end()){
                                        column_R.insert(j);
                                        column_index.conservativeResize(cc+1);
                                        column_index(cc)=j;
                                        cc++;
                                    }
                                }
                                else{
                                    v_ij = atof(token.c_str());
                                }
                                col++;
                            }
                            tripletList.push_back(T(i,j,v_ij));
                            row++;
                        }
                        R.setFromTriplets(tripletList.begin(), tripletList.end());
                        R_r=R.transpose();
			file.close();
                    }else{
                        cout << "Failed to read file " << file_name << endl;
                        return(0);
                    }

                }
                            return(1);
	}
    double rmse(SparseMatrix<double>& actual, MatrixXd& U, MatrixXd& M){
        unsigned int row =0;
    double error=0;
        for(int j =0; j< actual.cols();j++){
        for (SparseMatrix<double>::InnerIterator it(actual,j); it; ++it)
        {	
    double pred = U.col(((int)it.row())).dot(M.col((int)it.col())); //Pred(it.row(),it.col())
            double x = pred - it.value();
            error+=x*x;
        }
        }
        return error;
    }

                VectorXd ridge(MatrixXd& X, VectorXd& Y, double& lambda,int& k){
			MatrixXd temp1 = X*X.transpose();
                        MatrixXd temp2= MatrixXd::Identity(k,k);	
                        temp2 *= lambda;
                        temp1 += temp2;
                        return temp1.householderQr().solve(X*Y);
                }

                int main(int argc, const char* argv[]){
                    if(argc < 5)
                        cout << "Atleast 5 arguments" <<endl;
            int k = atoi(argv[1]);
                double lambda = atoi(argv[2]);
                int nr_threads = atoi(argv[3]);
                    const char* data = argv[4];
            read_data(data,k,lambda,nr_threads);
                    return(0);

                }

