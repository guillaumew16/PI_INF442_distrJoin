#include <iostream>
#include <string>
#include <sstream>

#include <cstdlib>
#include <ctime>

#include "relation.hpp"
#include "join.hpp"

using namespace std;

void demo_join() {
    Relation rel1 = fancyImport("rel1");
    Relation rel2 = fancyImport("rel2");

    clock_t begin = clock();

    Relation result = join(rel1, rel2);

    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;
    //if(rank == 0) {
	cout << endl << "Finished running demo_join. Execution time: " << elapsed_ms << "ms" << endl;
    //}

    cout << "arity of result: " << result.getArity() << endl;
    cout << "size of result: " << result.getSize() << endl;
    result.writeToFile("../output/join.txt");

    cout << "bonus: sort result before writeToFile so we can easily compare sequential vs. MPI results" << endl;
    //we will compare to, for example, "../output/MPIjoin_sorted.txt"
    Permutation identity(result.getArity());
    result.lexicoSort(identity);
    result.writeToFile("../output/join_sorted.txt");
}

void demo_triangle() {
    Relation rel = fancyImport("to triangle");

    clock_t begin = clock();

    Relation result = triangle(rel);

    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;
	cout << endl << "Finished running demo_triangle. Execution time: " << elapsed_ms << "ms" << endl;

    cout << "arity of result: " << result.getArity() << endl;
    cout << "size of result: " << result.getSize() << endl;
    result.writeToFile("../output/triangle.txt");

    cout << "bonus: sort result before writeToFile so we can easily compare sequential vs. MPI results" << endl;
    //we will compare to, for example, "../output/MPItriangle_sorted.txt"
    Permutation identity(result.getArity());
    result.lexicoSort(identity);
    result.writeToFile("../output/triangle_sorted.txt");
}

void demo_multijoin() {
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

    Relation result = multiJoin(input);
    
    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;
	cout << endl << "Finished running demo_multijoin. Execution time: " << elapsed_ms << "ms" << endl;

    cout << "arity of result: " << result.getArity() << endl;
    cout << "size of result: " << result.getSize() << endl;
    result.writeToFile("../output/multiJoin.txt");

    cout << "bonus: sort result before writeToFile so we can easily compare sequential vs. MPI results" << endl;
    //we will compare to, for example, "../output/MPImultijoin_sorted.txt"
    Permutation identity(result.getArity());
    result.lexicoSort(identity);
    result.writeToFile("../output/multiJoin_sorted.txt");
}

int main(int argc, char** argv) {
    string cmd;
    cout << "Choose among: join triangle multijoin" << endl;
    getline(cin, cmd);

    //apparently using switch with strings is not recommended
    if (cmd == "join") {
        demo_join();
    } else if (cmd == "triangle") {
        demo_triangle();
    } else if (cmd == "multijoin") {
        demo_multijoin();
    } else {
        cout << "Unrecognized cmd. Abort" << endl;
    }

    return 0;
}