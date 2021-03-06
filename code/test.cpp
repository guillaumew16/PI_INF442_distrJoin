#include <cstdlib>
#include <ctime>
#include <iostream>
#include <fstream>

#include "relation.hpp"
#include "join.hpp"

using namespace std;

int main(int argc, char** argv) {
    clock_t begin = clock();

	//Relation twitterRel("../data/twitter.dat", 2);
	//Relation facebookRel("../data/facebook.dat", 2);
	//Relation dblpRel("data/dblp.dat", 2);

	Relation twitterRel("../data_head/twitter.dat", 2);
	//Relation facebookRel("../data_head/facebook.dat", 2);
	//Relation dblpRel("../data_head/dblp.dat", 2);

	/*---------------------------------------*/
	/*---- import file and write to file ----*/
	/*
	Relation customRel("../data_head/custom.dat");
	customRel.head(20);
	customRel.writeToFile("../output/copy.txt");
	*/

	/*----------------------------*/
	/*---- lexicoSort twitter ----*/
	/*
	vector<int> permutVect(2);
	permutVect[0] = 1;
	permutVect[1] = 0;
	Permutation permutation(permutVect);

	cout << "begin lexicoSort" << endl;
	twitterRel.lexicoSort(permutation);
	twitterRel.head(20);
	twitterRel.writeToFile("../output/lexicoSort.txt");
	*/

	/*--------------------------------*/
	/*---- join dblp and twitter ----*/
	/*
	vector<int> z(2);
	z[0]=1;
	z[1]=3;
	vector<int> zp(2);
	zp[0]=3;
	zp[1]=4;

	dblpRel.setVariables(z);
	facebookRel.setVariables(zp);
	Relation result = join(dblpRel, facebookRel);
	*/

	/*--------------------------*/
	/*---- autoJoin twitter ----*/
	/*
	vector<int> z(2);
	z[0]=1;
	z[1]=3;
	vector<int> zp(2);
	zp[0]=3;
	zp[1]=4;

	twitterRel.setVariables(z);
	Relation result = autoJoin(twitterRel, zp);
	*/

	/*-----------------------------------*/
	/*---- find triangles in twitter ----*/
	
	vector<int> z(2);
	z[0]=1;
	z[1]=3;
	vector<int> zp(2);
	zp[0]=3;
	zp[1]=4;

	Relation result = triangle(twitterRel);
	

    /*----------------------------------------------*/
    /*---- naive find triangles using multiJoin ----*/
	/*
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

	vector<Relation> toJoin;
    toJoin.push_back(Relation(twitterRel));
	toJoin.push_back(Relation(twitterRel));
	toJoin.push_back(Relation(twitterRel));
	toJoin[0].setVariables(z12);
    toJoin[1].setVariables(z23);
    toJoin[2].setVariables(z13);

    Relation result = multiJoin(toJoin);

	//format triangle to avoid repeating entries
	result.formatTriangle();
*/
	/*-----------------------*/
	/*---- write to file ----*/

	cout<<"arity of result: "<<result.getArity() << endl;
	cout<<"size of result: "<<result.getSize() << endl;
    
	result.writeToFile("../output/test.txt");
	//result.writeToFile("../output/join.txt");
	//result.writeToFile("../output/triangle.txt");
	//result.writeToFile("../output/autoJoin.txt");

	//bonus: sort result before writeToFile so we can easily compare to result from sequential join
	//we will compare to, for example, "../output/autoJoin_sorted.txt"
	Permutation identity(result.getArity());
	result.lexicoSort(identity);
	result.writeToFile("../output/test_sorted.txt");
	//result.writeToFile("../output/join_sorted.txt");
	//result.writeToFile("../output/triangle_sorted.txt");
	//result.writeToFile("../output/autoJoin_sorted.txt");


	
	clock_t end = clock();
	double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;
	cout << endl << "Finished running main. Execution time: " << elapsed_ms << "ms" << endl;
    
	return 0;
}
