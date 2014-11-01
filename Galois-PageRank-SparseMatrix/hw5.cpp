#include "Galois/config.h"
#include "Galois/Galois.h"
#include "Galois/Accumulator.h"
#include "Galois/Bag.h"
#include "Galois/Statistic.h"
#include "Galois/Graph/LCGraph.h"
#include "Galois/Graph/TypeTraits.h"
#include <sys/stat.h>
#include "Lonestar/BoilerPlate.h"
#include "Galois/Timer.h"
#include GALOIS_CXX11_STD_HEADER(atomic)
#include <string>
#include <sstream>
#include <limits>
#include <iostream>
#include <fstream>
#include "hw5.h"

using namespace std;
using namespace Galois::Graph;
typedef LC_CSR_Graph<double, void> Graph;
typedef Graph::GraphNode GNode;
bool sortBefore(int i, int j);
vector<double> r(1770961);

struct P {
Graph& graph;
vector<double>& p;
vector<double>& r;
P(Graph& g, vector<double>& y,vector<double>& z): graph(g),p(y),r(z){ }
void operator()(const GNode& src, Galois::UserContext<GNode>& ctx) {
(*this)(src);
}
void operator()(const GNode& src) {
double sum = 0;
for (auto jj = graph.edge_begin(src, Galois::MethodFlag::NONE), ej = graph.edge_end(src, Galois::MethodFlag::NONE); jj != ej; ++jj) {
	GNode dst = graph.getEdgeDst(jj);
	sum += p[dst];
	}
	r[src]=sum;
}
};

struct Pre {
Graph& graph;
vector<double>& D;
vector<double>& r;
vector<double>& p;
double alpha=0.15;
Pre(Graph& g, vector<double>& y,vector<double>& z,vector<double>& q): graph(g),D(y),r(z),p(q){ }
void operator()(const GNode& src, Galois::UserContext<GNode>& ctx) {
(*this)(src);
}
void operator()(const GNode& src) {
    		p[src] = (1-alpha)*r[src]*D[src];
	}
};
struct Post {
Graph& graph;
vector<double>& r;
double n=0.15/1770961;
Post(Graph& g,vector<double>& z): graph(g),r(z){ }
void operator()(const GNode& src, Galois::UserContext<GNode>& ctx) {
(*this)(src);
}
void operator()(const GNode& src) {
                r[src] += n;
                }
     };

int main(int argc, char **argv) {
Galois::setActiveThreads(16);  
string filename = string(argv[1]);
typedef Galois::Graph::LC_CSR_Graph<double,void> Graph;
typedef Graph::GraphNode GNode;
typedef Graph::iterator iterator;
typedef Graph::edge_iterator edge_iterator;
Graph graph; 
Galois::Graph::readGraph(graph, filename);
int max_column=1770961;
// Problem 2
vector<double> p;
for(int i =0;i<max_column;i++)
	p.push_back((double)rand()/(double)RAND_MAX);
double value=0.0;
Galois::StatManager statManager;
for(int k=0;k<20;k++){
Galois::StatTimer T;
T.start();
Galois::for_each_local(graph, P(graph,p,r));
T.stop();
value+=T.get();
}
cout<<"Average time taken for multiply is "<<value/20000<<endl;

//Problem 3
vector<double> D(max_column);
double alpha=0.15;
double n = 1.0/max_column;
for (auto ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
double sum=0;
Graph::GraphNode src = *ii;
for (auto jj = graph.edge_begin(src, Galois::MethodFlag::NONE), ej = graph.edge_end(src, Galois::MethodFlag::NONE); jj != ej; ++jj) {
	sum+=1;
	}
D[src]=(1.0/sum);	
}
for (unsigned int i = 0; i != r.size(); ++i) r[i] = n;
//Galois::StatManager statManager;
Galois::StatTimer T;
T.start();
for(int i =0;i<50;i++){
Galois::do_all(graph.begin(),graph.end(),Pre(graph,D,r,p));
Galois::for_each_local(graph, P(graph,p,r));
Galois::do_all(graph.begin(),graph.end(),Post(graph,r));
}
T.stop();
value = T.get();
cout<<"Average time taken for 50 iterations is "<<value/1000<<endl;

vector<int>idx(max_column);
for (unsigned int i = 0; i != idx.size(); ++i) idx[i] = i;
sort(idx.begin(), idx.end(), sortBefore);
for(int rank=0;rank<10;rank++)
	cout<<rank+1<<"\t"<<idx[rank]+1<<"\t"<<r[idx[rank]]<<endl;

return 0;
}
bool sortBefore( int i, int j) {
  return r[i]> r[j];
}
