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

int h(unsigned int tohash, int m) {
	uint32_t seed = 42;

	unsigned int *out = (unsigned int *)malloc(16); //16 bytes = 128 bits
	//MurmurHash3_x86_32(const void *key, int len, uint32_t seed, void *out); //not sure if it's very bad to use this on a x64 computer..? <-- yes it is x)
	MurmurHash3_x64_128(&tohash, sizeof(unsigned int), seed, out); //this is probably uselessly expensive. we only get quarter of the output bits.

	int output = *out % m;
	free(out);

	//cout << "computed h(" << tohash << "," << m << ") = " << output << endl;
	return output;
}

Relation MPIjoin_fromfiles(const char *filename, const char *filenamep, vector<int> z, vector<int> zp, int root) { //parameter default: root=0
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

	cout << "running 'copydata' version of MPIjoin, but it was not tested at all!..." << endl
			<< "moreover, it is very very very probably bugged." << endl;
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
		vector<unsigned int> dummyVector(z.size(), 1); //define a well-defined dummy vector to avoid segfaults and the like
		//^-- I don't think this is necessary. TODO: try removing it and replacing by a simple unsigned int
		for (int m=0; m<numtasks; m++) {
			if (m == root) {
				cout << "^ from root: choosing not to send signal to root itself " << root << endl;
				continue;
			}
			cout << "^ from root: sending signal end of rel and relp entries to processor " << m << endl;
			MPI_Isend(&dummyVector[0], 1, MPI_UNSIGNED, m, 50, MPI_COMM_WORLD, &sendsignalReqs[m]); //tag 50: "signal end of rel and relp entries"
		}

		/*--------------------------------------------------------*/
		/*---- compute join of rootLocalRel and rootLocalRelp ----*/
		cout << "^ from root: compute join of rootLocalRel and rootLocalRelp" << endl;
		rootLocalRel.setVariables(z);
		rootLocalRelp.setVariables(zp);
		//Relation rootLocalJoin = join(rootLocalRel, rootLocalRelp);
		output = join(rootLocalRel, rootLocalRelp); //we would only have copied rootLocalJoin's entries into output anyway
		cout << "^^^ from root" << rank << ": rootLocalRel.getSize()=" << rootLocalRel.getSize() << "; rootLocalRelp.getSize()=" << rootLocalRelp.getSize() << "; output[for now].getSize()=" << output.getSize() << endl;

		/*---------------------------------------------------------*/
		/*---- receive answer entries from non-root processors ----*/
		cout << "^ from root: receive answer entries from non-root processors" << endl;
		MPI_Status status;
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

		string toPrint = "from machine"+to_string(rank)+": first entry of localRel:";
		printVector(localRel.getEntry(0), toPrint.c_str());
		toPrint = "from machine" + to_string(rank) + ": first entry of localRelp:";
		printVector(localRelp.getEntry(0), toPrint.c_str());
		printVector(z, "z:");
		printVector(zp, "zp:");

		Relation localJoin = join(localRel, localRelp);

		cout << "*** from machine " << rank << ": localRel.getSize()=" << localRel.getSize() << "; localRelp.getSize()=" << localRelp.getSize() << "; localJoin.getSize()=" << localJoin.getSize() << endl;

		/*-------------------------------------*/
		/*---- send back localJoin entries ----*/
		cout << "* from machine " << rank << ": send back localJoin entries" << endl;
		MPI_Request locSendentryReqs[localJoin.getSize()];
		int n = 0;
		for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
			int m = (*it)[k] % numtasks;
			unsigned int *toSend = &(*it)[0];
			MPI_Isend(toSend, localJoin.getArity(), MPI_UNSIGNED, root, 43, MPI_COMM_WORLD, &locSendentryReqs[n]); //tag 43: "data=entry for rel"
			n++;
		}

		/*-----------------------------------------------------------------*/
		/*---- send back dummy message to signal end of answer entries ----*/
		cout << "* from machine " << rank << ": send back dummy message to signal end of answer entries" << endl;
		MPI_Request locSignalReq;
		vector<unsigned int> dummyVector(z.size(), 1); //define a well-defined dummy vector to avoid segfaults and the like
		MPI_Isend(&dummyVector[0], 1, MPI_UNSIGNED, root, 53, MPI_COMM_WORLD, &locSignalReq); //tag 53: "signal end of answer entries"
	
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
	//TODO
	cout << "called \"copydata\" version of MPItriangle, but it is not coded yet. running \"nfs\" version of MPItriangle" << endl;

	cout << "MPI computing triangles of graph-relation..." << endl;
    if (rel.getArity() != 2) {
        cout << "| Error: input relation has arity != 2. Abort" << endl;
    }

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
	vector<int> z123(3);
	z123[0]=1;
	z123[1]=2;
	z123[2]=3;

	rel.setVariables(z12);
    Relation locIntermRel = MPIautoJoin(rel, z23, root);

	//since each processor has its own version of "output" in MPIjoin, root must broadcast actual result of join
	//to all other processors before we can continue.
	//alternatively, we could use the "copydata" version of MPIjoin, which passes all data (including inputs) over the network
	//the "copydata" version is actually the correct way to go with multi-way joins
	int joinArity = locIntermRel.getArity();
	int joinSize = locIntermRel.getSize(); //must do a copy since Relation::getXXX is const
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
	/*
	string filepath = "../output/MPItriangle-intermediary-" + to_string(rank) + ".txt";
	intermRel.writeToFile(filepath.c_str());
	*/

	intermRel.setVariables(z123);
	rel.setVariables(z13);
    Relation triangle = MPIjoin(intermRel, rel, root);

    return triangle;
}