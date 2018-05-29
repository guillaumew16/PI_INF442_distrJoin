#include <cstdlib>
#include <ctime>
#include <iostream>
#include <fstream>

#include "relation.hpp"

using namespace std;

int main(int argc, char** argv) {
    clock_t begin = clock();

	//Relation twitterRel("../data/twitter.dat", 2);
	//Relation facebookRel("../data/facebook.dat", 2);
	//Relation dblpRel("data/dblp.dat", 2);

	//Relation twitterRel("../data_head/twitter.dat", 2);
	//Relation facebookRel("../data_head/facebook.dat", 2);
	//Relation dblpRel("../data_head/dblp.dat", 2);

	/*---------------------------------------*/
	/*---- import file and write to file ----*/
	/*
	Relation customRel("../data_head/custom.dat", 5);
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

	/*-----------------------------------*/
	/*---- join facebook and twitter ----*/
	/*
	vector<int> z(2);
	z[0]=1;
	z[1]=3;
	vector<int> zp(2);
	zp[0]=3;
	zp[1]=4;

	twitterRel.setVariables(z);
	facebookRel.setVariables(zp);
	Relation outRel = join(twitterRel, facebookRel);
	outRel.writeToFile("../output/join.txt");
	*/	

	/*--------------------------*/
	/*---- autoJoin twitter ----*/
	
	vector<int> z(2);
	z[0]=1;
	z[1]=3;
	vector<int> zp(2);
	zp[0]=3;
	zp[1]=4;
	
	/*
	twitterRel.setVariables(z);
	Relation outRel = autoJoin(twitterRel, zp);
	outRel.writeToFile("../output/autoJoin.txt");
	*/


	//with pointers
	Relation *twitterRel;
	twitterRel = new Relation("../data_head/twitter.dat", 2);

	twitterRel->setVariables(z);
	Relation outRel = autoJoin(*twitterRel, zp);
	outRel.writeToFile("../output/autoJoin.txt");



    clock_t end = clock();
    double elapsed_ms = double((end - begin)*1000) / CLOCKS_PER_SEC;
	cout << endl << "Finished running main. Execution time: " << elapsed_ms << "ms" << endl;
    
	return 0;
}
