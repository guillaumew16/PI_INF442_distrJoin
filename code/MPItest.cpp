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
    cout << "file describing first relation:" << endl;
    cin >> filePath1;
    cout << "file describing second relation:" << endl;
    cin >> filePath2;

    vector<int> z(2);
    z[0]=1;
    z[1]=2;
    vector<int> zp(2);
    zp[0]=2;
    zp[1]=3;

    MPIjoin(filePath1.c_str(), filePath2.c_str(), z, zp);



    MPI_Finalize();

    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;
    if(rank == 0) {
	    cout << endl << "Finished running main. Execution time: " << elapsed_ms << "ms" << endl;
    }
	return 0;
}
