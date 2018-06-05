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

    /*------------------------*/
    /*---- basic MPI test ----*/
    /*
	int hostnamelen;
	char hostname[MPI_MAX_PROCESSOR_NAME];
	MPI_Get_processor_name(hostname, &hostnamelen);
    
	cout << "Hello from task " << rank << " on " << hostname << "!" << endl;
	if (rank == root) {
		cout << "root: Number of MPI tasks is " << numtasks << endl;
	}
    */

    /*----------------------------*/
    /*---- join two relations ----*/
    /*
    string filePath1, filePath2;
    // cout << "file describing first relation:" << endl;
    // cin >> filePath1;
    // cout << "file describing second relation:" << endl;
    //cin >> filePath2;
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
    */

	/*-----------------------------*/
	/*---- MPIautoJoin twitter ----*/
    /*
    Relation twitterRel("../data_head/twitter.dat", 2);
    vector<int> z(2);
	z[0]=1;
	z[1]=3;
	vector<int> zp(2);
	zp[0]=3;
	zp[1]=4;

	twitterRel.setVariables(z);
	Relation result = MPIautoJoin(twitterRel, zp);
    */

    /*------------------------------------------------*/
    /*---- find triangles in a relation using MPI ----*/
    /*
    string filePath1;
    // cout << "file describing relation in which we will find triangles:" << endl;
    // cin >> filePath1;
    filePath1 = "../data_head/twitter.dat";

    Relation rel(filePath1.c_str(), 2);
    Relation result = MPItriangle(rel);
    */

    /*--------------------------------------------------------------*/
    /*---- find triangles in a relation using hyperCubeTriangle ----*/
    /*
    string filePath1;
    // cout << "file describing relation in which we will find triangles:" << endl;
    // cin >> filePath1;
    filePath1 = "../data_head/twitter.dat";

    Relation rel(filePath1.c_str(), 2);
    Relation result = hyperCubeTriangle(rel);
    */

    /*-------------------------------------------------------*/
    /*---- naive find triangles using hyperCubeMultiJoin ----*/

    Relation rel("../data_head/twitter.dat");

	//it doesn't matter that z[i] is not in range [0, 2] (we assume global indexation of variables v_j)
	vector<int> z12(2);
	z12[0]=1;
	z12[1]=2;
	vector<int> z23(2);
	z23[0]=2;
	z23[1]=3;
	vector<int> z13(2);
	z13[0]=1;
	z13[1]=3;
	vector<int> z123(3);
	z123[0]=1;
	z123[1]=2;
	z123[2]=3;

	vector<Relation> toJoin;
    toJoin.push_back(Relation(rel));
    toJoin.push_back(Relation(rel));
    toJoin.push_back(Relation(rel));
    toJoin[0].setVariables(z12);
    toJoin[1].setVariables(z23);
    toJoin[2].setVariables(z13);

    Relation result = hyperCubeMultiJoin(toJoin);

	//format triangle to avoid repeating entries
    if (rank == root) {
	    result.formatTriangle();
    }

    /*-----------------------*/
    /*---- write to file ----*/

    if (rank == root) {
        cout<<"arity of result: "<<result.getArity() << endl;
        cout<<"size of result: "<<result.getSize() << endl;
        /*
        for (int i=0; i<20 && i<result.getSize(); i++) {
            cout << "dimension of entry " << i << ": " <<result.getEntry(i).size()<<endl;
        }
        */
        result.writeToFile("../output/MPItest.txt");
        //result.writeToFile("../output/MPIautojoin.txt");
        //result.writeToFile("../output/MPItriangle.txt");

        //bonus: sort result before writeToFile so we can easily compare to result from sequential join
        //we will compare to, for example, "../output/autoJoin_sorted.txt"
        Permutation identity(result.getArity());
        result.lexicoSort(identity);
        result.writeToFile("../output/MPItest_sorted.txt");
        //result.writeToFile("../output/MPIautojoin_sorted.txt");
        //result.writeToFile("../output/MPItriangle_sorted.txt");
    }



    MPI_Finalize();

    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;
    if(rank == 0) {
	    cout << endl << "Finished running main. Execution time: " << elapsed_ms << "ms" << endl;
    }
	return 0;
}
