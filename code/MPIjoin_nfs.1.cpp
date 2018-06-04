#include <cstdlib>
#include <stdexcept>
#include <iostream>
#include "mpi.h"
#include <string>

#include "MPIjoin.hpp"
#include "hash_function/MurmurHash3.h"
#include "join.hpp"
//#include "relation.hpp" <-- already done

using namespace std;

/*
int h(unsigned int tohash, int m) {
	return tohash % m;
}
*/

int h(unsigned int tohash, int m, uint32_t seed) { //parameter default: seed=42

	unsigned int *out = (unsigned int *)malloc(16); //16 bytes = 128 bits
	//MurmurHash3_x86_32(const void *key, int len, uint32_t seed, void *out); //not sure if it's very bad to use this on a x64 computer..? <-- yes it is x)
	MurmurHash3_x64_128(&tohash, sizeof(unsigned int), seed, out); //this is probably uselessly expensive. we only get quarter of the output bits.

	int output = *out % m;
	free(out);

	//cout << "computed h(" << tohash << "," << m << ") = " << output << endl;
	return output;
}

Relation MPIjoin_fromfiles(const char *filename, const char *filenamep, vector<int> z, vector<int> zp) {
	Relation rel(filename, z.size());
	Relation relp(filenamep, zp.size());
	rel.setVariables(z);
	relp.setVariables(zp);
	return MPIjoin(rel, relp);
}

Relation MPIjoin(Relation &rel, Relation &relp, int root) { //parameter default: root=0
	//we use MPI tags to signal processors the type of message they should expect: entry for rel or entry for relp

	//tags used:
	//(unused) 31: number of rel entries to be received
	//(unused) 32: number of relp entries to be received
	//(unused) 41: message contains a rel entry
	//(unused) 42: message contains a relp entry
	//(unused) 50: signals end of rel and relp entries
	//43: message contains an answer entry
	//53: signals end of answer entries (from processor status.MPI_SOURCE)
	//(unused) 20: signals end of MPIjoin (all answers have been received by root)

	//main phases:
	//- initialize stuff and import relations (for all processors, thanks to nfs or something like that)
	//- each processor determines which entries it should join
	//- each processor joins received rel and relp entries
	//- each processor Isends back result entries one by one, and Isends to root a dummy message with "signal end of answer entries" tag
	//- root (once it has Isent its result entries and signal message to itself) Recvs from all processors answer entries and signal messages
	//- root keeps track of which processor terminated and which did not (using status.MPI_SOURCE)
	//- root simply concatenates answer entries and returns.

	cout << "Starting MPIjoin(...). We assume MPI was initialized by caller (MPI_Init(...))." << endl;

	int rank, numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	if (root >= numtasks)
		throw invalid_argument("called MPIjoin with a root id >= numtasks");

	vector<int> z = rel.getVariables(); //do a copy
	vector<int> zp = relp.getVariables();

	int k=-1, kp; //choose one variable v in the intersection of lists of variables, v = z[k] = zp[kp]
	int c = 0; //number of common variables, to know what arity output will be
	for (int i=0; i<z.size(); i++) {
		for (int j=0; j<zp.size(); j++) {
			if (z[i] == zp[j]) {
				if (k == -1) { // k and kp have not yet been set
					k = i;
					kp = j;
				}
				c++;
			}
		}
	}
	int joinArity = z.size() + zp.size() - c;
	Relation output(joinArity);

	
	if (rank == root) {
		cout << "we determined z[k] = zp[kp] with k=" << k << " and kp=" << kp << endl;
		cout << "z[k]: " << z[k] << "; zp[kp]: " << zp[kp] << endl;
	}
	

	/*----------------------------------------------------------*/
	/*---- compute which entries this processor should join ----*/
	Relation locRel(z.size());
	for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
		if ( h((*it)[k], numtasks) == rank ) {
			locRel.addEntry(*it);
		}
	}
	locRel.setVariables(z);
	Relation locRelp(zp.size());
	for (vector<vector<unsigned int> >::iterator it=relp.getBegin(); it!=relp.getEnd(); it++) {
		if ( h((*it)[kp], numtasks) == rank ) {
			locRelp.addEntry(*it);
		}
	}
	locRelp.setVariables(zp);
	
	//cout << "from machine " << rank << ": locRel has size "<< locRel.getSize() << "; locRelp has size " << locRelp.getSize() << endl;

	/*----------------------------*/
	/*---- compute local join ----*/
	Relation localJoin = join(locRel, locRelp, 0); //0 for non-verbose

	if (localJoin.getArity() != joinArity) {
		//cout << "Detected logic error on machine " << rank << ":" << endl;
		//cout << "| locally computed join localJoin.getArity() = " << localJoin.getArity() << endl;
		//cout << "| previously computed joinArity = " << joinArity << endl;
		throw logic_error("after computing localJoin, we find that localJoin.getArity() != z.size() + zp.size() - c");
	}

	//cout << "from machine "<<rank << ": locaJoin has arity " << localJoin.getArity() << ", and size " << localJoin.getSize() << endl;

	/*-------------------------------------*/
	/*---- send back localJoin entries ----*/
	if (rank == root) {
		//root doesn't Send to itself to avoid deadlock
		for (vector<vector<unsigned int>>::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
			output.addEntry(*it);
		}
	} else {
		MPI_Request locSendentryReqs[localJoin.getSize()];
		//int n = 0;
		for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
			//cout << "from machine "<<rank << ": sending an answer entry" << endl;
			//int m = h((*it)[k], numtasks);
			unsigned int *toSend = &(*it)[0];
			//blocking send to make sure all entries are sent before sending "end of answer entries" signal
			MPI_Send(toSend, localJoin.getArity(), MPI_UNSIGNED, root, 43, MPI_COMM_WORLD /*, &locSendentryReqs[n]*/); //tag 41: "data=answer entry"
			//n++;
		}
	}

	/*-------------------------------------------------------------------*/
	/*---- send back dummy message to signal "end of answer entries" ----*/
	MPI_Request locSignalReq;
	unsigned int dummyValue = 42;
	MPI_Isend(&dummyValue, 1, MPI_UNSIGNED, root, 53, MPI_COMM_WORLD, &locSignalReq); //tag 53: "signal end of answer entries"

	//cout<<"from machine " << rank << ": finished Isending localJoin entries, as well as signal \"end of answer entries\"." << endl;

	//work of non-root processors ends here.

	if (rank == root) {
		/*------------------------------------------------*/
		/*---- receive and concatenate answer entries ----*/

		MPI_Status status;
		for (int m=0; m<numtasks; m++) {
			if (m == root) { //root doesn't Send to itself to avoid deadlock
				continue;
			}
			while (status.MPI_TAG != 53) { //tag 53: "signal end of answer entries"
				vector<unsigned int> recvEntry(joinArity);
				//blocking receive, so that we continue only when all non-root processors sent "end of answer entries" tag
				MPI_Recv(&recvEntry[0], joinArity, MPI_UNSIGNED, m, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				if (status.MPI_TAG == 43) { //tag 43: "data=answer entry"
					output.addEntry(recvEntry);
					//printVector(recvEntry, "received answer entry");
					//cout << "root received answer entry from " << m << endl;
				} else if (status.MPI_TAG != 53) {
					string errMsg = "root received a message with tag " + to_string(status.MPI_TAG) + ", while expecting answer entries (tag 43 or 53)";
					printVector(recvEntry, errMsg.c_str());
					throw logic_error(errMsg.c_str());
				}
			}
			cout << "root received \"end of answer entries\" signal from " << m << endl;
			status.MPI_TAG = 1234; //any value != 53
		}
	}

	vector<int> newZ = localJoin.getVariables(); //do a copy
	output.setVariables(newZ);

	return output;
}

Relation MPIautoJoin(Relation &rel, vector<int> &zp, int root) { //parameter default: root=0
	if (rel.getArity() != zp.size())
		throw invalid_argument("called MPIautoJoin for rel with zp, zp has size != arity of rel");

	Relation relp(rel);
	relp.setVariables(zp);
	return MPIjoin(rel, relp, root);
}

Relation MPItriangle(Relation &rel, int root) { //parameter default: root=0
	//both joins of the multi-way join will be performed with the same root processor

	cout << "MPI computing triangles of graph-relation..." << endl;
    if (rel.getArity() != 2)
        throw invalid_argument("called MPItriangle on input relation with arity != 2");

	int rank, numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	if (root >= numtasks)
		throw invalid_argument("called MPItriangle with a root id >= numtasks");

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

	rel.setVariables(z12);
	Relation locIntermRel = MPIautoJoin(rel, z23, root);

	//since each processor has its own version of "output" in MPIjoin, root must broadcast actual result of join
	//to all other processors before we can continue.
	int joinArity = locIntermRel.getArity();
	int joinSize = locIntermRel.getSize();
	vector<int> joinVariables = locIntermRel.getVariables(); //must do a copy since Relation::getXXX is const
	MPI_Bcast(&joinSize, 1, MPI_INT, root, MPI_COMM_WORLD); //root broadcasts nb of entries to be received

	Relation intermRel(joinArity);
	vector<unsigned int> recvbuffer(joinArity);
	for (int i=0; i<joinSize; i++) {
		if (rank == root) {
			recvbuffer = locIntermRel.getEntry(i);
		}
		MPI_Bcast(&recvbuffer[0], joinArity, MPI_UNSIGNED ,root, MPI_COMM_WORLD);
		intermRel.addEntry(recvbuffer); //addEntry adds a copy
	}
	intermRel.setVariables(joinVariables); //don't forget to set variables!

	/*
	string filepath = "../output/MPItriangle-intermediary-" + to_string(rank) + ".txt";
	intermRel.writeToFile(filepath.c_str());
	*/

	rel.setVariables(z13);
    Relation triangle = MPIjoin(intermRel, rel, root);

	if (rank == root) {
		//format triangle to avoid repeating entries
		triangle.formatTriangle();
	}

	return triangle;
}

Relation hyperCubeMultiJoin(vector<Relation> toJoin, int root) { //parameter default: root=0
	//joins toJoin's elements in the order in which they appear in toJoin:
	// (((toJoin[0] >< toJoin[1]) >< toJoin[2]) >< ...)

	//tags used:
	//(unused) 31: number of rel entries to be received
	//(unused) 32: number of relp entries to be received
	//(unused) 41: message contains a rel entry
	//(unused) 42: message contains a relp entry
	//(unused) 50: signals end of rel and relp entries
	//43: message contains an answer entry
	//53: signals end of answer entries (from processor status.MPI_SOURCE)
	//(unused) 20: signals end of MPIjoin (all answers have been received by root)

	cout << "called hyperCubeMultiJoin, but is not yet tested..." << endl;
			<< "not even finished coding!" << endl;

	cout << "Using HyperCube method to compute a multi-way join. We assume MPI was initialized by caller (MPI_Init(...))." << endl;
	if (rel.getArity() != 2)
		throw invalid_argument("called hyperCubeTriangle on input relation with arity != 2");

	int rank, numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	if (root >= numtasks)
		throw invalid_argument("called hyperCubeTriangle with a root id >= numtasks");

	cout << "for now we assume that numtasks = 3*3*3 = 27" << endl;
	if (numtasks != 27)
		throw logic_error("called hyperCubeTriangle but numtasks != 27 (not really a logic_error but whatever)");

	/*--------------------------------------------------------*/
	/*---- compute processor's address (m1, m2, ..., m_k) ----*/
	//if rank = m0_m1_m2 in base 3, then m = [m0, m1, m2]
	int m[3];
	m[2] = rank%3;
	m[1] = (rank/3)%3;
	m[0] = (rank/3)/3;
	/*
	string toPrint = "from machine " + to_string(rank) + ": address array m=";
	vector<int> mVect = {m[0], m[1], m[2]};
	printVector(mVect, toPrint.c_str());
	*/

	/*-----------------------------------------------------------------*/
	/*---- setup list of variables, seed and other stuff for later ----*/
	vector<int> z01(2);
	z01[0]=0;
	z01[1]=1;
	vector<int> z12(2);
	z12[0]=1;
	z12[1]=2;
	vector<int> z02(2);
	z02[0]=0;
	z02[1]=2;

	int seed[3];
	seed[0]=1234;
	seed[1]=2345;
	seed[2]=3456;

	int joinArity = 3;
	Relation output(joinArity);

	/*----------------------------------------------------------*/
	/*---- compute which entries this processor should join ----*/
	//we see rel as an atom over the variables (x0, x1)
	Relation locRel01(2);
	locRel01.setVariables(z01);
	for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
		if ( h((*it)[0], 3, seed[0]) == m[0] && h((*it)[1], 3, seed[1]) == m[1] ) {
			locRel01.addEntry(*it);
		}
	}

	//we see rel as an atom over the variables (x1, x2)
	Relation locRel12(2);
	locRel12.setVariables(z12);
	for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
		if ( h((*it)[0], 3, seed[1]) == m[1] && h((*it)[1], 3, seed[2]) == m[2] ) {
			locRel12.addEntry(*it);
		}
	}

	//we see rel as an atom over the variables (x0, x2)
	Relation locRel02(2);
	locRel02.setVariables(z02);
	for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
		if ( h((*it)[0], 3, seed[0]) == m[0] && h((*it)[1], 3, seed[2]) == m[2] ) {
			locRel02.addEntry(*it);
		}
	}
	/*
	cout << "from machine " << rank << ": locRel01 has size "<< locRel01.getSize() 
			<< "; locRel12 has size " << locRel12.getSize() 
			<< "; locRel02 has size " << locRel02.getSize() << endl;
	*/

	/*----------------------------*/
	/*---- compute local join ----*/
	//this will do copies of locRelxy, which is inefficient. for correctness testing we leave it like this for clarity
	//TODO: change this when testing for performance
	vector<Relation> toJoin; 
	toJoin.push_back(locRel01);
	toJoin.push_back(locRel12);
	toJoin.push_back(locRel02);
	Relation localJoin = multiJoin(toJoin);

	if (localJoin.getArity() != joinArity) {
		//cout << "Detected logic error on machine " << rank << ":" << endl;
		//cout << "| locally computed join localJoin.getArity() = " << localJoin.getArity() << endl;
		//cout << "| since we are computing HyperCube triangle, joinArity should be = 3" << endl;
		throw logic_error("in hyperCubeTriangle, after computing multiJoin locally, we find that localJoin.getArity() != 3");
	}

	//cout << "from machine "<<rank << ": localJoin has arity " << localJoin.getArity() << ", and size " << localJoin.getSize() << endl;



	string filepath = "../output/run1/hyperCubetriangle-localJoin-" + to_string(rank) + ".txt";
	localJoin.writeToFile(filepath.c_str());
	cout << "*** from machine " << rank << ": localJoin.getSize()=" << localJoin.getSize() << endl;


	/*-------------------------------------*/
	/*---- send back localJoin entries ----*/

	if (rank == root) {
		//root doesn't Send to itself to avoid deadlock
		for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
			output.addEntry(*it);
		}
	} else {
		MPI_Request locSendentryReqs[localJoin.getSize()];
		int n = 0;
		for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
			//cout << "from machine "<<rank << ": sending an answer entry" << endl;
			//int m = h((*it)[k], numtasks);
			unsigned int *toSend = &(*it)[0];
			//blocking send to make sure all entries are sent before sending "end of answer entries" signal
			MPI_Isend(toSend, localJoin.getArity(), MPI_UNSIGNED, root, 43, MPI_COMM_WORLD, &locSendentryReqs[n]); //tag 41: "data=answer entry"
			n++;
		}
		MPI_Waitall(n, locSendentryReqs, MPI_STATUSES_IGNORE);
	}


	/*-------------------------------------------------------------------*/
	/*---- send back dummy message to signal "end of answer entries" ----*/
	if (rank != root) {
		//root doesn't send to itself to avoid deadlock
		MPI_Request locSignalReq;
		unsigned int dummyValue = 42;
		MPI_Isend(&dummyValue, 1, MPI_UNSIGNED, root, 53, MPI_COMM_WORLD, &locSignalReq); //tag 53: "signal end of answer entries"
	}
	//cout<<"from machine " << rank << ": finished Isending localJoin entries, as well as signal \"end of answer entries\"." << endl;

	//work of non-root processors ends here.

	if (rank == root) {
		/*------------------------------------------------*/
		/*---- receive and concatenate answer entries ----*/

		vector<int> nbEntriesRecvd(numtasks, 0);

		MPI_Status status;
		status.MPI_TAG = 1234;
		for (int m=0; m<numtasks; m++) {
			if (m == root) { //root doesn't Send to itself to avoid deadlock
				continue;
			}
			while (status.MPI_TAG != 53) { //tag 53: "signal end of answer entries"
				vector<unsigned int> recvEntry(joinArity);
				//blocking receive, so that we continue only when all non-root processors sent "end of answer entries" tag
				MPI_Recv(&recvEntry[0], joinArity, MPI_UNSIGNED, m, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				if (status.MPI_TAG == 43) { //tag 43: "data=answer entry"
					output.addEntry(recvEntry);
					//printVector(recvEntry, "received answer entry");
					//cout << "root received answer entry from " << m << endl;
					nbEntriesRecvd[m] ++;
				} else if (status.MPI_TAG != 53) {
					string errMsg = "root received a message with tag " + to_string(status.MPI_TAG) + ", while expecting answer entries (tag 43 or 53)";
					printVector(recvEntry, errMsg.c_str());
					throw logic_error(errMsg.c_str());
				}
			}
			cout << "root received \"end of answer entries\" signal from " << m << "; received " << nbEntriesRecvd[m] << " answer entries from it" << endl;
			status.MPI_TAG = 1234; //any value != 53
		}
	}

	vector<int> newZ = localJoin.getVariables(); //do a copy
	output.setVariables(newZ);

	if (rank == root) {
		//format triangle to avoid repeating entries
		output.formatTriangle();
	}

	cout << "from machine " << rank << ": locRel01 has size " << locRel01.getSize()
		 << "; locRel12 has size " << locRel12.getSize()
		 << "; locRel02 has size " << locRel02.getSize() << endl;
	cout << "from machine " << rank << ": localJoin has arity " << localJoin.getArity() << ", and size " << localJoin.getSize() << endl;

	return output;
}

Relation hyperCubeTriangle(Relation &rel, int root) { //parameter default: root=0
	//tags used:
	//(unused) 31: number of rel entries to be received
	//(unused) 32: number of relp entries to be received
	//(unused) 41: message contains a rel entry
	//(unused) 42: message contains a relp entry
	//(unused) 50: signals end of rel and relp entries
	//43: message contains an answer entry
	//53: signals end of answer entries (from processor status.MPI_SOURCE)
	//(unused) 20: signals end of MPIjoin (all answers have been received by root)

	cout << "Using HyperCube method to compute triangles of graph-relation. We assume MPI was initialized by caller (MPI_Init(...))." << endl;
	if (rel.getArity() != 2)
		throw invalid_argument("called hyperCubeTriangle on input relation with arity != 2");

	int rank, numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	if (root >= numtasks)
		throw invalid_argument("called hyperCubeTriangle with a root id >= numtasks");

	cout << "for now we assume that numtasks = 3*3*3 = 27" << endl;
	if (numtasks != 27)
		throw logic_error("called hyperCubeTriangle but numtasks != 27 (not really a logic_error but whatever)");

	/*--------------------------------------------------------*/
	/*---- compute processor's address (m1, m2, ..., m_k) ----*/
	//if rank = m0_m1_m2 in base 3, then m = [m0, m1, m2]
	int m[3];
	m[2] = rank%3;
	m[1] = (rank/3)%3;
	m[0] = (rank/3)/3;
	/*
	string toPrint = "from machine " + to_string(rank) + ": address array m=";
	vector<int> mVect = {m[0], m[1], m[2]};
	printVector(mVect, toPrint.c_str());
	*/

	/*-----------------------------------------------------------------*/
	/*---- setup list of variables, seed and other stuff for later ----*/
	vector<int> z01(2);
	z01[0]=0;
	z01[1]=1;
	vector<int> z12(2);
	z12[0]=1;
	z12[1]=2;
	vector<int> z02(2);
	z02[0]=0;
	z02[1]=2;

	int seed[3];
	seed[0]=1234;
	seed[1]=2345;
	seed[2]=3456;

	int joinArity = 3;
	Relation output(joinArity);

	/*----------------------------------------------------------*/
	/*---- compute which entries this processor should join ----*/
	//we see rel as an atom over the variables (x0, x1)
	Relation locRel01(2);
	locRel01.setVariables(z01);
	for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
		if ( h((*it)[0], 3, seed[0]) == m[0] && h((*it)[1], 3, seed[1]) == m[1] ) {
			locRel01.addEntry(*it);
		}
	}

	//we see rel as an atom over the variables (x1, x2)
	Relation locRel12(2);
	locRel12.setVariables(z12);
	for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
		if ( h((*it)[0], 3, seed[1]) == m[1] && h((*it)[1], 3, seed[2]) == m[2] ) {
			locRel12.addEntry(*it);
		}
	}

	//we see rel as an atom over the variables (x0, x2)
	Relation locRel02(2);
	locRel02.setVariables(z02);
	for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
		if ( h((*it)[0], 3, seed[0]) == m[0] && h((*it)[1], 3, seed[2]) == m[2] ) {
			locRel02.addEntry(*it);
		}
	}
	/*
	cout << "from machine " << rank << ": locRel01 has size "<< locRel01.getSize() 
			<< "; locRel12 has size " << locRel12.getSize() 
			<< "; locRel02 has size " << locRel02.getSize() << endl;
	*/

	/*----------------------------*/
	/*---- compute local join ----*/
	//this will do copies of locRelxy, which is inefficient. for correctness testing we leave it like this for clarity
	//TODO: change this when testing for performance
	vector<Relation> toJoin; 
	toJoin.push_back(locRel01);
	toJoin.push_back(locRel12);
	toJoin.push_back(locRel02);
	Relation localJoin = multiJoin(toJoin);

	if (localJoin.getArity() != joinArity) {
		//cout << "Detected logic error on machine " << rank << ":" << endl;
		//cout << "| locally computed join localJoin.getArity() = " << localJoin.getArity() << endl;
		//cout << "| since we are computing HyperCube triangle, joinArity should be = 3" << endl;
		throw logic_error("in hyperCubeTriangle, after computing multiJoin locally, we find that localJoin.getArity() != 3");
	}

	//cout << "from machine "<<rank << ": localJoin has arity " << localJoin.getArity() << ", and size " << localJoin.getSize() << endl;



	string filepath = "../output/run1/hyperCubetriangle-localJoin-" + to_string(rank) + ".txt";
	localJoin.writeToFile(filepath.c_str());
	cout << "*** from machine " << rank << ": localJoin.getSize()=" << localJoin.getSize() << endl;


	/*-------------------------------------*/
	/*---- send back localJoin entries ----*/

	if (rank == root) {
		//root doesn't Send to itself to avoid deadlock
		for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
			output.addEntry(*it);
		}
	} else {
		MPI_Request locSendentryReqs[localJoin.getSize()];
		int n = 0;
		for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
			//cout << "from machine "<<rank << ": sending an answer entry" << endl;
			//int m = h((*it)[k], numtasks);
			unsigned int *toSend = &(*it)[0];
			//blocking send to make sure all entries are sent before sending "end of answer entries" signal
			MPI_Isend(toSend, localJoin.getArity(), MPI_UNSIGNED, root, 43, MPI_COMM_WORLD, &locSendentryReqs[n]); //tag 41: "data=answer entry"
			n++;
		}
		MPI_Waitall(n, locSendentryReqs, MPI_STATUSES_IGNORE);
	}


	/*-------------------------------------------------------------------*/
	/*---- send back dummy message to signal "end of answer entries" ----*/
	if (rank != root) {
		//root doesn't send to itself to avoid deadlock
		MPI_Request locSignalReq;
		unsigned int dummyValue = 42;
		MPI_Isend(&dummyValue, 1, MPI_UNSIGNED, root, 53, MPI_COMM_WORLD, &locSignalReq); //tag 53: "signal end of answer entries"
	}
	//cout<<"from machine " << rank << ": finished Isending localJoin entries, as well as signal \"end of answer entries\"." << endl;

	//work of non-root processors ends here.

	if (rank == root) {
		/*------------------------------------------------*/
		/*---- receive and concatenate answer entries ----*/

		vector<int> nbEntriesRecvd(numtasks, 0);

		MPI_Status status;
		status.MPI_TAG = 1234;
		for (int m=0; m<numtasks; m++) {
			if (m == root) { //root doesn't Send to itself to avoid deadlock
				continue;
			}
			while (status.MPI_TAG != 53) { //tag 53: "signal end of answer entries"
				vector<unsigned int> recvEntry(joinArity);
				//blocking receive, so that we continue only when all non-root processors sent "end of answer entries" tag
				MPI_Recv(&recvEntry[0], joinArity, MPI_UNSIGNED, m, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				if (status.MPI_TAG == 43) { //tag 43: "data=answer entry"
					output.addEntry(recvEntry);
					//printVector(recvEntry, "received answer entry");
					//cout << "root received answer entry from " << m << endl;
					nbEntriesRecvd[m] ++;
				} else if (status.MPI_TAG != 53) {
					string errMsg = "root received a message with tag " + to_string(status.MPI_TAG) + ", while expecting answer entries (tag 43 or 53)";
					printVector(recvEntry, errMsg.c_str());
					throw logic_error(errMsg.c_str());
				}
			}
			cout << "root received \"end of answer entries\" signal from " << m << "; received " << nbEntriesRecvd[m] << " answer entries from it" << endl;
			status.MPI_TAG = 1234; //any value != 53
		}
	}

	vector<int> newZ = localJoin.getVariables(); //do a copy
	output.setVariables(newZ);

	if (rank == root) {
		//format triangle to avoid repeating entries
		output.formatTriangle();
	}

	cout << "from machine " << rank << ": locRel01 has size " << locRel01.getSize()
		 << "; locRel12 has size " << locRel12.getSize()
		 << "; locRel02 has size " << locRel02.getSize() << endl;
	cout << "from machine " << rank << ": localJoin has arity " << localJoin.getArity() << ", and size " << localJoin.getSize() << endl;

	return output;
}
