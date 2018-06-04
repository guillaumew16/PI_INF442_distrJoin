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

Relation MPIjoin_fromfiles(const char *filename, const char *filenamep, vector<int> z, vector<int> zp, int root) { //parameter default: root=0
	//only root needs to actually import the files.

	cout << "Starting MPIjoin(...). We assume MPI was initialized by caller (MPI_Init(...))." << endl;

	int rank, numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	if (root >= numtasks)
		throw invalid_argument("called MPIjoin with a root id >= numtasks");

	Relation *rel;
	Relation *relp;
	if (rank == root) { //in MPIjoin, rel and relp are ignored by non-root processors
		rel = new Relation(filename, z.size());
		relp = new Relation(filenamep, zp.size());
	} else {
		rel = new Relation(z.size());
		relp = new Relation(zp.size());
	}
	rel->setVariables(z);
	relp->setVariables(zp);
	Relation output = MPIjoin(*rel, *relp);
	delete rel;
	delete relp;
	return output;
}

Relation MPIjoin(Relation &rel, Relation &relp, int root) { //parameter default: root=0
	//parameters rel and relp are ignored by non-root processors (except to get z and zp)

	//we ended up completely separating code for root processor and for non-root, which is somewhat strange for a MPI program
	//this is because when we tried putting code in common, we couldn't get rid of deadlock problems
	//so we decided to send no MPI message from root to itself, to avoid deadlock

	//we use MPI tags to signal processors the type of message they should expect: entry for rel or entry for relp
	
	//tags used:
	//(unused) 31: number of rel entries to be received
	//(unused) 32: number of relp entries to be received
	//41: message contains a rel entry
	//42: message contains a relp entry
	//50: signal end of rel and relp entries
	//43: message contains an answer entry
	//53: signal end of answer entries (from processor status.MPI_SOURCE)
	//(unused) 20: signal end of MPIjoin (all answers have been received by root)

	//main phases:
	//- initialize stuff (only for root)
	//- root Isends rel and relp entries one by one to non-root chosen processor
	//- entries that are to be treated by root are stored locally (no MPI message from root to itself, to avoid deadlock)
	//- each processor Recvs rel and relp entries
	//- root waits until all non-root processors received all entries
	//- root Isends dummy message with "signal end of rel and relp entries" tag
	//- each processor joins received rel and relp entries. root does too (in a distinct loop)
	//- each processor Isends back result entries one by one, and Isends to root a dummy message with "signal end of answer entries" tag
	//- root Recvs from all processors answer entries and signal messages
	//- root can keep track of which processor terminated and which did not (using status.MPI_SOURCE)
	//- root simply concatenates answer entries and returns.

	//we could not use Bcast to broadcast "signal end of rel and relp entries" because of the way MPI works: you don't Recv a Bcast, everyone has to call Bcast
	//but what we need is a way to send the signal to the processors, even as they are ready to receive new entries

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
	cout << "joinArity: " << joinArity << endl;
	Relation output(joinArity);

	if (rank == root) {
		
		Relation rootLocalRel(z.size());
		Relation rootLocalRelp(zp.size());

		/*-----------------------------------*/
		/*---- send rel and relp entries ----*/
		cout << "^ from root: send rel and relp entries" << endl;

		//Nonblocking calls allocate a communication request object and associate it with the request handle (the argument request).
		//The request can be used later to query the status of the communication or wait for its completion.
		MPI_Request sendentryReqs[rel.getSize()];
		int nbEntryReqs=0;
		for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
			int m = h((*it)[k], numtasks);
			if (m == root) {
				//store locally
				rootLocalRel.addEntry(*it);
			} else {
				unsigned int *toSend = &(*it)[0]; //convert vector to array
				MPI_Isend(toSend, rel.getArity(), MPI_UNSIGNED, m, 41, MPI_COMM_WORLD, &sendentryReqs[nbEntryReqs]); //tag 41: "data=entry for rel"
				nbEntryReqs++;
			}
		}

		MPI_Request sendentryReqsp[relp.getSize()];
		int nbEntryReqsp = 0;
		for (vector<vector<unsigned int> >::iterator it = relp.getBegin(); it != relp.getEnd(); it++) {
			int m = h((*it)[kp], numtasks);
			if (m == root) {
				rootLocalRelp.addEntry(*it);
			} else {
				unsigned int *toSend = &(*it)[0];
				MPI_Isend(toSend, relp.getArity(), MPI_UNSIGNED, m, 42, MPI_COMM_WORLD, &sendentryReqsp[nbEntryReqsp]); //tag 42: "data=entry for relp"
				nbEntryReqsp++;
			}
		}

		/*----------------------------------------------------------------*/
		/*---- wait until all rel and relp entries have been received ----*/
		cout << "^ from root: wait until all rel and relp entries have been received" << endl;
		MPI_Waitall(nbEntryReqs, sendentryReqs, MPI_STATUSES_IGNORE);
		MPI_Waitall(nbEntryReqsp, sendentryReqsp, MPI_STATUSES_IGNORE);

		/*-------------------------------------------------------------------*/
		/*---- send dummy messages to signal end of rel and relp entries ----*/
		cout << "^ from root: send dummy messages to signal end of rel and relp entries" << endl;
		MPI_Request sendsignalReqs[numtasks]; //MPI_Isend requires a MPI_Request parameter, but we don't actually use this
		unsigned int dummyValue = 42;
		for (int m=0; m<numtasks; m++) {
			if (m == root) {
				cout << "^ from root: choosing not to send signal to root itself " << root << endl;
				continue;
			}
			cout << "^ from root: sending signal end of rel and relp entries to processor " << m << endl;
			MPI_Isend(&dummyValue, 1, MPI_UNSIGNED, m, 50, MPI_COMM_WORLD, &sendsignalReqs[m]); //tag 50: "signal end of rel and relp entries"
		}

		/*--------------------------------------------------------*/
		/*---- compute join of rootLocalRel and rootLocalRelp ----*/
		cout << "^ from root: compute join of rootLocalRel and rootLocalRelp" << endl;
		rootLocalRel.setVariables(z);
		rootLocalRelp.setVariables(zp);
		//Relation rootLocalJoin = join(rootLocalRel, rootLocalRelp, 0); //0 for non-verbose
		output = join(rootLocalRel, rootLocalRelp, 0); //we would only have copied rootLocalJoin's entries into output anyway
		
		cout << "^^^ from root" << rank << ": rootLocalRel.getSize()=" << rootLocalRel.getSize() << "; rootLocalRelp.getSize()=" << rootLocalRelp.getSize() << "; output[for now].getSize()=" << output.getSize() << endl;

		/*-------------------------------------------------------------------------*/
		/*---- receive and concatenate answer entries from non-root processors ----*/
		cout << "^ from root: receive and concatenate answer entries from non-root processors" << endl;
		MPI_Status status;
		status.MPI_TAG = 1234;
		for (int m=0; m<numtasks; m++) {
			if (m == root) {
				continue;
			}
			while (status.MPI_TAG != 53) { //tag 53: "signal end of answer entries"
				vector<unsigned int> recvEntry(joinArity);
				//blocking receive, so that we continue only when all non-root processors sent "signal end of answer entries" tag
				MPI_Recv(&recvEntry[0], joinArity, MPI_UNSIGNED, m, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				if (status.MPI_TAG == 43) { //tag 43: "data=answer entry"
					output.addEntry(recvEntry);
				}
			}
			status.MPI_TAG = 1234; //any value != 53
		}

	 	//end "if (rank == root)"
	} else {
			
		/*-----------------------------------*/
		/*---- recv rel and relp entries ----*/
		cout << "* from machine " << rank << ": recv rel and relp entries" << endl;

		//local variables
		Relation localRel(z.size());
		Relation localRelp(zp.size());
		int maxArity = max(z.size(), zp.size());

		MPI_Status status;
		status.MPI_TAG = 1;

		while (status.MPI_TAG != 50) { //tag 50: "signal end of rel and relp entries"
			vector<unsigned int> recvEntry(maxArity);
			//standard-mode blocking receive
			MPI_Recv(&recvEntry[0], maxArity, MPI_UNSIGNED, root, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			if (status.MPI_TAG == 41) { //tag 41: "data=entry for rel"
				//cout << "* from machine " << rank << ": received a rel entry" << endl;
				recvEntry.resize(z.size());
				localRel.addEntry(recvEntry);
			} else if (status.MPI_TAG == 42) { //tag 42: "data=entry for relp"
				//cout << "* from machine " << rank << ": received a relp entry" << endl;
				recvEntry.resize(zp.size());
				localRelp.addEntry(recvEntry);
			}
			// else { cout << "from machine " << rank << ": received a message with tag " << status.MPI_TAG << endl; }
		}

		/*------------------------------------------------*/
		/*---- compute join of localRel and localRelp ----*/
		cout << "* from machine " << rank << ": compute join of localRel and localRelp" << endl;
		localRel.setVariables(z);
		localRelp.setVariables(zp);
		/*
		string toPrint = "from machine"+to_string(rank)+": first entry of localRel:";
		printVector(localRel.getEntry(0), toPrint.c_str());
		toPrint = "from machine" + to_string(rank) + ": first entry of localRelp:";
		printVector(localRelp.getEntry(0), toPrint.c_str());
		printVector(z, "z:");
		printVector(zp, "zp:");
		*/
		Relation localJoin = join(localRel, localRelp, 0); //0 for non-verbose

		cout << "*** from machine " << rank << ": localRel.getSize()=" << localRel.getSize() << "; localRelp.getSize()=" << localRelp.getSize() << "; localJoin.getSize()=" << localJoin.getSize() << endl;

		/*-------------------------------------*/
		/*---- send back localJoin entries ----*/
		cout << "* from machine " << rank << ": send back localJoin entries" << endl;
		MPI_Request locSendentryReqs[localJoin.getSize()];
		int n = 0;
		for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
			unsigned int *toSend = &(*it)[0];
			MPI_Isend(toSend, localJoin.getArity(), MPI_UNSIGNED, root, 43, MPI_COMM_WORLD, &locSendentryReqs[n]); //tag 43: "data=answer entry"
			n++;
		}

		/*-----------------------------------------------------------------*/
		/*---- send back dummy message to signal end of answer entries ----*/
		cout << "* from machine " << rank << ": send back dummy message to signal end of answer entries" << endl;
		unsigned int dummyValue = 42;
		//blocking Send so that we don't terminate before root is ready to terminate
		MPI_Send(&dummyValue, 1, MPI_UNSIGNED, root, 53, MPI_COMM_WORLD); //tag 53: "signal end of answer entries"

		//we return empty output but must have correct variables
		//no need to do this for root since in "if (rank == root)" we did output = join(...)
		vector<int> newZ = localJoin.getVariables(); //do a copy
		output.setVariables(newZ);
	}

	return output;
}

Relation MPIautoJoin(Relation &rel, vector<int> &zp, int root) { //parameter default: root=0
	if (rel.getArity() != zp.size())
		throw invalid_argument("called MPIautoJoin for rel with zp, zp has size != arity of rel");

	Relation relp(rel);
	relp.setVariables(zp);
	return MPIjoin(rel, relp, root);
}

Relation MPItriangle(Relation &rel, int root) {
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
    Relation intermRel = MPIautoJoin(rel, z23, root);
	
	string filepath = "../output/MPItriangle-intermediary-" + to_string(rank) + ".txt";
	intermRel.writeToFile(filepath.c_str());
	

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

	cout << "called hyperCubeMultiJoin for method \"copydata\", but is not yet implemented. Returning dummy value" << endl;
	return Relation(1);
}

Relation hyperCubeTriangle(Relation &rel, int root) { //default parameter: root=0
	//tags used:
	//401: message contains a rel01 entry
	//412: message contains a rel12 entry
	//402: message contains a rel02 entry
	//500: signal end of relxy entries
	//499: message contains an answer entry
	//599: signal end of answer entries (from processor status.MPI_SOURCE)

	//main phases:
	//- initialize stuff (only for root)
	//- root Isends rel and relp entries one by one to non-root chosen processor
	//- entries that are to be treated by root are stored locally (no MPI message from root to itself, to avoid deadlock)
	//- each processor Recvs rel and relp entries
	//- root waits until all non-root processors received all entries
	//- root Isends dummy message with "signal end of rel and relp entries" tag
	//- each processor joins received rel and relp entries. root does too (in a distinct loop)
	//- each processor Isends back result entries one by one, and Isends to root a dummy message with "signal end of answer entries" tag
	//- root Recvs from all processors answer entries and signal messages
	//- root can keep track of which processor terminated and which did not (using status.MPI_SOURCE)
	//- root simply concatenates answer entries and returns.

	cout << "calling hyperCubeTriangle, but was not tested at all!..." << endl;

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

	if (rank == root) {
		Relation rootLocalRel01(2);
		Relation rootLocalRel12(2);
		Relation rootLocalRel02(2);
		rootLocalRel01.setVariables(z01);
		rootLocalRel12.setVariables(z12);
		rootLocalRel02.setVariables(z02);

		/*------------------------------*/
		/*---- precompute addresses ----*/
		int m[3][3][3];
		for (int i=0; i<3; i++) {
			for (int j=0; j<3; j++) {
				for (int k=0; k<3; k++) {
					//if rank = m[i][j][k], then rank = i_j_k in base 3
					m[i][j][k] = k + 3*j + 3*3*i;
					/*if (rank == root) {
						cout << i << j << k << "=" << m[i][j][k] << endl;
					}*/
				}
			}
		}

		/*----------------------------*/
		/*---- send rel01 entries ----*/
		cout << "^ from root: send rel01 entries" << endl;
		//we see rel as an atom over the variables (x0, x1)

		//Nonblocking calls allocate a communication request object and associate it with the request handle (the argument request).
		//The request can be used later to query the status of the communication or wait for its completion.
		MPI_Request sendentryReqs01[joinArity * rel.getSize()];
		int nbEntryReqs01=0;
		for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
			unsigned int *toSend = &(*it)[0]; //convert vector to array

			//find addresses of processors to send to, considering that we consider variables (x0, x1)
			// [ h0(it[0]) , h1(it[1]) , 0 ]
			// [ h0(it[0]) , h1(it[1]) , 1 ]
			// [ h0(it[0]) , h1(it[1]) , 2 ]
			int dest[3];
			dest[0] = m[h((*it)[0], 3, seed[0])]
						[h((*it)[1], 3, seed[1])]
						[0];
			dest[1] = m[h((*it)[0], 3, seed[0])]
						[h((*it)[1], 3, seed[1])]
						[1];
			dest[2] = m[h((*it)[0], 3, seed[0])]
						[h((*it)[1], 3, seed[1])]
						[2];

			//cout << endl << "^^^ from root: sendentryReqs01 was allocated size rel.getSize()=" << rel.getSize() << endl;
			for (int i=0; i<3; i++) {
				if (dest[i] == root) {
					//store locally
					rootLocalRel01.addEntry(*it);
				} else {
					MPI_Isend(toSend, 2, MPI_UNSIGNED, dest[i], 401, MPI_COMM_WORLD, &sendentryReqs01[nbEntryReqs01]); //tag 401: "data=entry for rel01"
					nbEntryReqs01++;
				}
			}
		}

		/*----------------------------*/
		/*---- send rel12 entries ----*/
		cout << "^ from root: send rel12 entries" << endl;
		//we see rel as an atom over the variables (x1, x2)
		MPI_Request sendentryReqs12[joinArity * rel.getSize()];
		int nbEntryReqs12=0;
		for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
			unsigned int *toSend = &(*it)[0];
			// [ 0 , h1(it[0]) , h2(it[1]) ]
			// [ 1 , h1(it[0]) , h2(it[1]) ]
			// [ 2 , h1(it[0]) , h2(it[1]) ]
			int dest[3];
			dest[0] = m[0]
					   [h((*it)[0], 3, seed[1])]
					   [h((*it)[1], 3, seed[2])];
			dest[1] = m[1]
					   [h((*it)[0], 3, seed[1])]
					   [h((*it)[1], 3, seed[2])];
			dest[2] = m[2]
					   [h((*it)[0], 3, seed[1])]
					   [h((*it)[1], 3, seed[2])];
			for (int i=0; i<3; i++) {
				if (dest[i] == root) {
					//store locally
					rootLocalRel12.addEntry(*it);
				} else {
					MPI_Isend(toSend, 2, MPI_UNSIGNED, dest[i], 412, MPI_COMM_WORLD, &sendentryReqs12[nbEntryReqs12]); //tag 412: "data=entry for rel12"
					nbEntryReqs12++;
				}
			}
		}

		/*----------------------------*/
		/*---- send rel02 entries ----*/
		cout << "^ from root: send rel02 entries" << endl;
		//we see rel as an atom over the variables (x0, x2)
		MPI_Request sendentryReqs02[joinArity * rel.getSize()];
		int nbEntryReqs02=0;
		for (vector<vector<unsigned int> >::iterator it=rel.getBegin(); it!=rel.getEnd(); it++) {
			unsigned int *toSend = &(*it)[0];
			//find addresses of processors to send to, considering that we consider variables (x0, x2)
			// [ h0(it[0]) , 0 , h2(it[1]) ]
			// [ h0(it[0]) , 1 , h2(it[1]) ]
			// [ h0(it[0]) , 2 , h2(it[1]) ]
			int dest[3];
			dest[0] = m[h((*it)[0], 3, seed[0])]
					   [0]
					   [h((*it)[1], 3, seed[2])];
			dest[1] = m[h((*it)[0], 3, seed[0])]
					   [1]
					   [h((*it)[1], 3, seed[2])];
			dest[2] = m[h((*it)[0], 3, seed[0])]
					   [2]
					   [h((*it)[1], 3, seed[2])];

			for (int i=0; i<3; i++) {
				if (dest[i] == root) {
					//store locally
					rootLocalRel02.addEntry(*it);
				} else {
					MPI_Isend(toSend, 2, MPI_UNSIGNED, dest[i], 402, MPI_COMM_WORLD, &sendentryReqs02[nbEntryReqs02]); //tag 402: "data=entry for rel02"
					nbEntryReqs02++;
				}
			}
		}

		/*---------------------------------------------------------*/
		/*---- wait until all relxy entries have been received ----*/
		cout << "^ from root: wait until all rel entries have been received" << endl;
		MPI_Waitall(nbEntryReqs01, sendentryReqs01, MPI_STATUSES_IGNORE);
		MPI_Waitall(nbEntryReqs12, sendentryReqs12, MPI_STATUSES_IGNORE);
		MPI_Waitall(nbEntryReqs02, sendentryReqs02, MPI_STATUSES_IGNORE);

		/*------------------------------------------------------------*/
		/*---- send dummy messages to signal end of relxy entries ----*/
		cout << "^ from root: send dummy messages to signal end of rel (and relp) entries" << endl;
		MPI_Request sendsignalReqs[numtasks]; //MPI_Isend requires a MPI_Request parameter, but we don't actually use this
		unsigned int dummyValue = 42;
		for (int m=0; m<numtasks; m++) {
			if (m == root) {
				cout << "^ from root: choosing not to send signal to root itself " << root << endl;
				continue;
			}
			cout << "^ from root: sending signal end of rel and relp entries to processor " << m << endl;
			MPI_Isend(&dummyValue, 1, MPI_UNSIGNED, m, 500, MPI_COMM_WORLD, &sendsignalReqs[m]); //tag 500: "signal end of relxy entries"
		}

		/*---------------------------------------------*/
		/*---- compute triangle from data for root ----*/
		cout << "^ from root: compute triangles from data for root" << endl;

		//this will do copies of locRelxy, which is inefficient. for correctness testing we leave it like this for clarity
		//TODO: change this when testing for performance
		vector<Relation> toJoin;
		toJoin.push_back(rootLocalRel01);
		toJoin.push_back(rootLocalRel12);
		toJoin.push_back(rootLocalRel02);
		//Relation rootLocalJoin = multiJoin(toJoin);
		output = multiJoin(toJoin); //we would only have copied rootLocalJoin's entries into output anyway
		
		if (output.getArity() != joinArity) {
			//cout << "Detected logic error on machine " << rank << ":" << endl;
			//cout << "| locally computed join localJoin.getArity() = " << localJoin.getArity() << endl;
			//cout << "| since we are computing HyperCube triangle, joinArity should be = 3" << endl;
			throw logic_error("from root: in hyperCubeTriangle, after computing multiJoin locally, we find that rootLocalJoin.getArity() != 3");
		}

		/*-------------------------------------------------------------------------*/
		/*---- receive and concatenate answer entries from non-root processors ----*/
		cout << "^ from root: receive and concatenate answer entries from non-root processors" << endl;
		MPI_Status status;
		status.MPI_TAG = 1234;
		for (int m=0; m<numtasks; m++) {
			if (m == root) {
				continue;
			}
			while (status.MPI_TAG != 599) { //tag 599: "signal end of answer entries"
				vector<unsigned int> recvEntry(joinArity);
				//blocking receive, so that we continue only when all non-root processors sent "signal end of answer entries" tag
				MPI_Recv(&recvEntry[0], joinArity, MPI_UNSIGNED, m, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				if (status.MPI_TAG == 499) { //tag 499: "data=answer entry"
					output.addEntry(recvEntry);
				}
			}
			status.MPI_TAG = 1234; //any value != 599
		}

	 	//end "if (rank == root)"
	} else {
			
		/*----------------------------*/
		/*---- recv relxy entries ----*/
		cout << "* from machine " << rank << ": recv relxy entries" << endl;

		//local variables
		Relation localRel01(2);
		Relation localRel12(2);
		Relation localRel02(2);
		localRel01.setVariables(z01);
		localRel12.setVariables(z12);
		localRel02.setVariables(z02);
		int maxArity = 2;

		MPI_Status status;
		status.MPI_TAG = 1;

		while (status.MPI_TAG != 500) { //tag 500: "signal end of relxy entries"
			vector<unsigned int> recvEntry(maxArity);
			//standard-mode blocking receive
			MPI_Recv(&recvEntry[0], maxArity, MPI_UNSIGNED, root, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			if (status.MPI_TAG == 401) { //tag 401: "data=entry for rel01"
				//cout << "* from machine " << rank << ": received a rel01 entry" << endl;
				localRel01.addEntry(recvEntry);
			} else if (status.MPI_TAG == 412) {
				//cout << "* from machine " << rank << ": received a rel12 entry" << endl;
				localRel12.addEntry(recvEntry);
			} else if (status.MPI_TAG == 402) {
				//cout << "* from machine " << rank << ": received a rel02 entry" << endl;
				localRel02.addEntry(recvEntry);
			} else if (status.MPI_TAG != 500 ) { 
				cout << "Warning: from machine " << rank << ": received a message with tag " << status.MPI_TAG << endl; 
			}
		}

		/*-----------------------------------------------------*/
		/*---- compute triangle from data for this machine ----*/
		cout << "* from machine " << rank << ": compute triangle from data for this machine" << endl;

		//this will do copies of locRelxy, which is inefficient. for correctness testing we leave it like this for clarity
		//TODO: change this when testing for performance
		vector<Relation> toJoin;
		toJoin.push_back(localRel01);
		toJoin.push_back(localRel12);
		toJoin.push_back(localRel02);
		Relation localJoin = multiJoin(toJoin);
		
		if (localJoin.getArity() != joinArity) {
			//cout << "Detected logic error on machine " << rank << ":" << endl;
			//cout << "| locally computed join localJoin.getArity() = " << localJoin.getArity() << endl;
			//cout << "| since we are computing HyperCube triangle, joinArity should be = 3" << endl;
			throw logic_error("in hyperCubeTriangle, after computing multiJoin locally, we find that localJoin.getArity() != 3");
		}

		/*-------------------------------------*/
		/*---- send back localJoin entries ----*/
		cout << "* from machine " << rank << ": send back localJoin entries" << endl;
		MPI_Request locSendentryReqs[localJoin.getSize()];
		int n = 0;
		for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
			unsigned int *toSend = &(*it)[0];
			MPI_Isend(toSend, joinArity, MPI_UNSIGNED, root, 499, MPI_COMM_WORLD, &locSendentryReqs[n]); //tag 499: "data=answer entry"
			n++;
		}

		/*-----------------------------------------------------------------*/
		/*---- send back dummy message to signal end of answer entries ----*/
		cout << "* from machine " << rank << ": send back dummy message to signal end of answer entries" << endl;
		unsigned int dummyValue = 42;
		//blocking Send so that we don't terminate before root is ready to terminate
		MPI_Send(&dummyValue, 1, MPI_UNSIGNED, root, 599, MPI_COMM_WORLD); //tag 599: "signal end of answer entries"
		
		//we return empty output but must have correct variables
		//no need to do this for root since in "if (rank == root)" we did output = multiJoin(...)
		vector<int> newZ = localJoin.getVariables(); //do a copy
		output.setVariables(newZ);
	}

	if (rank == root) {
		//format triangle to avoid repeating entries
		output.formatTriangle();
	}

	return output;
}