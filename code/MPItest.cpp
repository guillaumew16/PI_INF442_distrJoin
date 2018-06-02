#include <cstdlib>
#include <ctime>
#include <iostream>
#include <fstream>
#include <string>
#include "mpi.h"

#include "MPIjoin.hpp"

using namespace std;

int main(int argc, char** argv) {
    clock_t begin = clock();

    //initialize MPI
    int rc;
    rc = MPI_Init(&argc, &argv);
    if(rc != MPI_SUCCESS) {
        cout <<"Error starting MPI program. Terminating.\n";
        MPI_Abort(MPI_COMM_WORLD, rc);
    }
	int rank, numtasks;
	const int root = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);


    //basic MPI test
    /*
	int hostnamelen;
	char hostname[MPI_MAX_PROCESSOR_NAME];
	MPI_Get_processor_name(hostname, &hostnamelen);
    
	cout << "Hello from task " << rank << " on " << hostname << "!" << endl;
	if (rank == root) {
		cout << "root: Number of MPI tasks is " << numtasks << endl;
	}
    */

    string filePath1, filePath2;
    /*
    cout << "file describing first relation:" << endl;
    cin >> filePath1;
    cout << "file describing second relation:" << endl;
    cin >> filePath2;
    */
    filePath1 = "../data_head/dblp.dat";
    filePath2 = "../data_head/facebook.dat";

    vector<int> z(2);
    z[0]=1;
    z[1]=2;
    vector<int> zp(2);
    zp[0]=2;
    zp[1]=3;

    Relation rel(filePath1.c_str(), 2);
    Relation relp(filePath2.c_str(), 2);
    rel.setVariables(z);
    relp.setVariables(zp);

    //cout << "from machine " << rank << ": rel.size() = " << rel.getSize() << " and relp.size() = " << relp.getSize() << endl;

    Relation result = MPIjoin(rel, relp);
    //Relation result = MPIjoin_file(filePath1.c_str(), filePath2.c_str(), z, zp);

    if (rank == root) {
        cout<<"arity of result: "<<result.getArity() << endl;
        cout<<"size of result: "<<result.getSize() << endl;
        /*
        for (int i=0; i<20 && i<result.getSize(); i++) {
            cout << "dimension of entry " << i << ": " <<result.getEntry(i).size()<<endl;
        }
        */
        result.writeToFile("../output/MPItest.txt");

        //bonus: sort result before writeToFile so we can easily compare to result from sequential join
        //we will compare to, for example, "../output/autoJoin_sorted.txt"
        Permutation identity(result.getArity());
        result.lexicoSort(identity);
        result.writeToFile("../output/MPItest_sorted.txt");
    }

    MPI_Finalize();

    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;
    if(rank == 0) {
	    cout << endl << "Finished running main. Execution time: " << elapsed_ms << "ms" << endl;
    }
	return 0;
}
