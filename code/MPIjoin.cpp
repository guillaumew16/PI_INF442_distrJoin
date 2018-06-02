#include <cstdlib>
#include <stdexcept>
#include <iostream>
#include "mpi.h"
#include <string>

#include "MPIjoin.hpp"
#include "hash_function/MurmurHash3.h"

//#include "relation.hpp" <-- already done

using namespace std;

/*
int h(unsigned int tohash, int len) {
	return tohash % len;
}
*/

int h(unsigned int tohash, int len) {
	int *out = (int*)malloc(16);
	uint32_t seed = 42;
	//MurmurHash3_x86_32(&tohash, len, seed, &out); //not sure if it's very bad to use this on a x64 computer..? <-- yes it is x)
	MurmurHash3_x64_128(&tohash, len, seed, &out); //this is probably uselessly expensive. we only get quarter of the output bits.
	
	cout << "computed h("<<tohash<<","<<len<<") = "<<*out%len<<endl;
	
	int output = *out % len;
	free(out);
	return output;
}

Relation MPIjoin(Relation &rel, Relation &relp) {
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

	cout << "running 'normal' version of MPIjoin, but it was not tested at all!..." << endl;
	cout << "Starting MPIjoin(...). We assume MPI was initialized by caller (MPI_Init(...))." << endl;

	int rank, numtasks;
	const int root = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	
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

	/*
	if (rank == root) {
		cout << "we determined z[k] = zp[kp] with k=" << k << " and kp=" << kp << endl;
		cout << "z[k]: " << z[k] << "; zp[kp]: " << zp[kp] << endl;
	}
	*/

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
	MPI_Request locSendentryReqs[localJoin.getSize()];
	int n = 0;
	for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
		//cout << "from machine "<<rank << ": sending an answer entry" << endl;
		//int m = h((*it)[k], numtasks);
		unsigned int *toSend = &(*it)[0];
		//blocking send to make sure all entries are sent before sending "end of answer entries" signal
		MPI_Send(toSend, localJoin.getArity(), MPI_UNSIGNED, root, 43, MPI_COMM_WORLD /*, &locSendentryReqs[n]*/); //tag 41: "data=answer entry"
		n++;
	}

	/*-------------------------------------------------------------------*/
	/*---- send back dummy message to signal "end of answer entries" ----*/
	MPI_Request locSignalReq;
	vector<unsigned int> dummyVector(z.size(), 1); //define a well-defined dummy vector to avoid segfaults and the like
	MPI_Isend(&dummyVector[0], 1, MPI_UNSIGNED, root, 53, MPI_COMM_WORLD, &locSignalReq); //tag 53: "signal end of answer entries"

	//cout<<"from machine " << rank << ": finished Isending localJoin entries, as well as signal \"end of answer entries\"." << endl;

	//work of non-root processors ends here.

	if (rank == root) {
		/*------------------------------------------------*/
		/*---- receive and concatenate answer entries ----*/

		MPI_Status status;
		for (int m=0; m<numtasks; m++) {
			while (status.MPI_TAG != 53) { //tag 53: "signal end of answer entries"
				vector<unsigned int> recvEntry(joinArity);
				//blocking receive, so that we continue only when all non-root processors sent "end of answer entries" tag
				MPI_Recv(&recvEntry[0], joinArity, MPI_UNSIGNED, m, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				if (status.MPI_TAG == 43) { //tag 43: "data=answer entry"
					output.addEntry(recvEntry);
					//printVector(recvEntry, "received answer entry");
					//cout << "root received answer entry from " << m << endl;
				} else if (status.MPI_TAG != 53) {
					string errMsg = "root received a message with tag " + to_string(status.MPI_TAG) + ", while excepting answer entries (tag 43 or 53)";
					printVector(recvEntry, errMsg.c_str());
					throw logic_error(errMsg.c_str());
				}
			}
			//cout << "root received \"end of answer entries\" signal from " << m << endl;
			status.MPI_TAG = 1234; //any value != 53
		}

		vector<int> newZ = localJoin.getVariables(); //do a copy
		output.setVariables(newZ);
	}

	return output;
}
