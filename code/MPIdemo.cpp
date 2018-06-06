#include <iostream>
#include <string>
#include <sstream>

#include <cstdlib>
#include <ctime>
#include "mpi.h"

#include "relation.hpp"
#include "MPIjoin.hpp"

using namespace std;

void demo_MPIjoin() {
	int rank, numtasks;
	const int root = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

    Relation rel1 = fancyImport("rel1");
    Relation rel2 = fancyImport("rel2");

    clock_t begin = clock();

    Relation result = MPIjoin(rel1, rel2);

    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;

    if (rank == 0) {
	    cout << endl << "Finished running demo_MPIjoin. Execution time: " << elapsed_ms << "ms" << endl;

        cout << "arity of result: " << result.getArity() << endl;
        cout << "size of result: " << result.getSize() << endl;
        result.writeToFile("../output/MPIjoin.txt");

        cout << "bonus: sort result before writeToFile so we can easily compare sequential vs. MPI results" << endl;
        //we will compare to, for example, "../output/join_sorted.txt"
        Permutation identity(result.getArity());
        result.lexicoSort(identity);
        result.writeToFile("../output/MPIjoin_sorted.txt");
    }
}

void demo_MPItriangle() {
	int rank, numtasks;
	const int root = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

    Relation rel = fancyImport("to triangle");

    clock_t begin = clock();

    Relation result = MPItriangle(rel);

    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;

    if (rank == 0) {
	    cout << endl << "Finished running demo_MPItriangle. Execution time: " << elapsed_ms << "ms" << endl;

        cout << "arity of result: " << result.getArity() << endl;
        cout << "size of result: " << result.getSize() << endl;
        result.writeToFile("../output/MPItriangle.txt");

        cout << "bonus: sort result before writeToFile so we can easily compare sequential vs. MPI results" << endl;
        //we will compare to, for example, "../output/triangle_sorted.txt"
        Permutation identity(result.getArity());
        result.lexicoSort(identity);
        result.writeToFile("../output/MPItriangle_sorted.txt");
    }
}

void demo_hypercubetriangle() {
	int rank, numtasks;
	const int root = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

    Relation rel = fancyImport("to triangle");

    clock_t begin = clock();

    Relation result = MPItriangle(rel);

    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;

    if (rank == 0) {
	    cout << endl << "Finished running demo_hypercubetriangle. Execution time: " << elapsed_ms << "ms" << endl;

        cout << "arity of result: " << result.getArity() << endl;
        cout << "size of result: " << result.getSize() << endl;
        result.writeToFile("../output/HCtriangle.txt");

        cout << "bonus: sort result before writeToFile so we can easily compare sequential vs. MPI results" << endl;
        //we will compare to, for example, "../output/triangle_sorted.txt"
        Permutation identity(result.getArity());
        result.lexicoSort(identity);
        result.writeToFile("../output/HCtriangle_sorted.txt");
    }
}

void demo_hypercubemultijoin() {
	int rank, numtasks;
	const int root = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

    //import multiple relations
    cout << "number of relations to multijoin: " << endl;
    string nbStr;
    getline(cin, nbStr);
    int nb = stoi(nbStr);
    vector<Relation> input;
    input.reserve(nb);

    for (int i=0; i<nb; i++) {
        input.push_back(fancyImport("#"+to_string(i)));
    }

    clock_t begin = clock();

    Relation result = hyperCubeMultiJoin(input);
    
    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;

    if (rank == 0) {
	    cout << endl << "Finished running demo_hypercubetriangle. Execution time: " << elapsed_ms << "ms" << endl;

        cout << "arity of result: " << result.getArity() << endl;
        cout << "size of result: " << result.getSize() << endl;
        result.writeToFile("../output/HCmultijoin.txt");

        cout << "bonus: sort result before writeToFile so we can easily compare sequential vs. MPI results" << endl;
        //we will compare to, for example, "../output/multijoin_sorted.txt"
        Permutation identity(result.getArity());
        result.lexicoSort(identity);
        result.writeToFile("../output/HCmultijoin_sorted.txt");
    }
}

int main(int argc, char** argv) {
    cout << "MPIdemo not implemented... Abort" << endl;
    //choosing which file to import with MPI is hard!!
    return 0;

    //for record:

    //initialize MPI
    int rc;
    rc = MPI_Init(&argc, &argv);
    if(rc != MPI_SUCCESS) {
        cout <<"Error starting MPI program. Terminating.\n";
        MPI_Abort(MPI_COMM_WORLD, rc);
    }
    
    string cmd;
    cout << "Choose among: MPIjoin MPItriangle hypercubetriangle" << endl;
    getline(cin, cmd);

    //apparently using switch with strings is not recommended
    if (cmd == "MPIjoin") {
        demo_MPIjoin();
    } else if (cmd == "MPItriangle") {
        demo_MPItriangle();
    } else if (cmd == "hypercubetriangle") {
        demo_hypercubetriangle();
    } else if (cmd == "hypercubemultijoin") {
        cout << "Please be advised that this function currently doesn't work (problem with choice of processor)..." << endl;
        demo_hypercubemultijoin();
    } else {
        cout << "Unrecognized cmd. Abort" << endl;
    }

    MPI_Finalize();
    return 0;
}