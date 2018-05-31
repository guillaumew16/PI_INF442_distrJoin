#include <cstdlib>
#include <stdexcept>
#include <iostream>
#include "mpi.h"

#include "MPIjoin.hpp"
//#include "relation.hpp" <-- already done

using namespace std;

Relation MPIjoin(const char *filename, const char *filenamep, vector<int> z, vector<int> zp) {
	//we use MPI tags to signal processors the type of message they should expect: entry for rel or entry for relp
	
	//tags used:
	//(unused) 31: number of rel entries to be received
	//(unused) 32: number of relp entries to be received
	//41: message contains a rel entry
	//42: message contains a relp entry
	//50: signals end of rel and relp entries
	//43: message contains an answer entry
	//53: signals end of answer entries (from processor status.MPI_SOURCE)
	//(unused) 20: signals end of MPIjoin (all answers have been received by root)

	//main phases:
	//- initialize stuff (only for root)
	//- root Isends rel and relp entries one by one to chosen processors, and each processor Recvs rel and relp entries
	//- root Isends dummy message with "signal end of rel and relp entries" tag
	//- each processor joins received rel and relp entries
	//- each processor Isends back result entries one by one, and Isends to root a dummy message with "signal end of answer entries" tag
	//- root (once it has Isent its result entries and signal message to itself) Recvs from all processors answer entries and signal messages
	//- root keeps track of which processor terminated and which did not (using status.MPI_SOURCE)
	//- root simply concatenates answer entries and returns.

	//we could not use Bcast to broadcast "signal end of rel/relp" because of the way MPI works: you don't Recv a Bcast, everyone has to call Bcast
	//but what we need is a way to send the signal to the processors, even as they are ready to receive new entries

	cout << "running 'copydata' version of MPIjoin, but it was not tested at all!..." << endl;
	//moreover, it is very very very probably bugged.
	cout << "Starting MPIjoin(...). We assume MPI was initialized by caller (MPI_Init(...))." << endl;

	int rank, numtasks;
	const int root = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	
	//root-specific variables
	//use pointers because these need to be declared outside "if" to have correct scope, in the point of view of root processor
	//TODO: check that it is indeed the case that we need to access them in multiple "ifs"
	Relation *rel;
	Relation *relp;
	MPI_Request *sendentryReqs;
	MPI_Request *sendentryReqsp;

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
		/*----------------------------------------*/
		/*---- setup variables needed by root ----*/
		rel = new Relation(filename, z.size());
		relp = new Relation(filenamep, zp.size());
		rel->setVariables(z);
		relp->setVariables(zp);

		/*-----------------------------------*/
		/*---- send rel and relp entries ----*/

		//Nonblocking calls allocate a communication request object and associate it with the request handle (the argument request).
		//The request can be used later to query the status of the communication or wait for its completion. 
		sendentryReqs = (MPI_Request *) malloc(rel->getSize()); //C-style but no choice
		int n=0;
		for (vector<vector<unsigned int> >::iterator it=rel->getBegin(); it!=rel->getEnd(); it++) {
			int m = (*it)[k] % numtasks;
			unsigned int *toSend = &(*it)[0]; //convert vector to array
			MPI_Isend(toSend, rel->getArity(), MPI_UNSIGNED, m, 41, MPI_COMM_WORLD, &sendentryReqs[n]); //tag 41: "data=entry for rel"
			n++;
		}

		sendentryReqsp = (MPI_Request*) malloc(relp->getSize());
		n = 0;
		for (vector<vector<unsigned int> >::iterator it = relp->getBegin(); it != relp->getEnd(); it++) {
			int m = (*it)[kp] % numtasks;
			unsigned int *toSend = &(*it)[0];
			MPI_Isend(toSend, relp->getArity(), MPI_UNSIGNED, m, 42, MPI_COMM_WORLD, &sendentryReqsp[n]); //tag 42: "data=entry for relp"
			n++;
		}


		/*---------------------------------------------------------*/
		/*---- receive answer entries from non-root processors ----*/
		//we cannot expect to receive answer entries from root as well, because Recv from root is blocking
		//we will Recv answer entries from root later

		MPI_Status status;
		for (int m=0; m<numtasks; m++) {
			if (m == root) {
				continue;
			}
			while (status.MPI_TAG != 53) { //tag 53: "signal end of answer entries"
				vector<unsigned int> recvEntry(joinArity);
				//blocking receive, so that we continue only when all non-root processors sent "end of answer entries" tag
				MPI_Recv(&recvEntry[0], joinArity, MPI_UNSIGNED, m, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				if (status.MPI_TAG == 43) { //tag 43: "data=answer entry"
					output.addEntry(recvEntry);
				}
			}
		}
	}

	/*-----------------------------------*/
	/*---- recv rel and relp entries ----*/
	
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
			recvEntry.resize(z.size());
			localRel.addEntry(recvEntry);
		} else if (status.MPI_TAG == 42) { //tag 42: "data=entry for relp"
			recvEntry.resize(zp.size());
			localRelp.addEntry(recvEntry);
		}
	}

	/*------------------------------------------------*/
	/*---- compute join of localRel and localRelp ----*/
	Relation localJoin = join(localRel, localRelp);

	/*-------------------------------------*/
	/*---- send back localJoin entries ----*/

	MPI_Request locSendentryReqs[localJoin.getSize()];
	int n = 0;
	for (vector<vector<unsigned int> >::iterator it = localJoin.getBegin(); it != localJoin.getEnd(); it++) {
		int m = (*it)[k] % numtasks;
		unsigned int *toSend = &(*it)[0];
		MPI_Isend(toSend, localJoin.getArity(), MPI_UNSIGNED, root, 43, MPI_COMM_WORLD, &locSendentryReqs[n]); //tag 43: "data=entry for rel"
		n++;
	}

	/*-------------------------------------------------------------------*/
	/*---- send back dummy message to signal "end of answer entries" ----*/
	MPI_Request locSignalReq;
	vector<unsigned int> dummyVector(z.size(), 1); //define a well-defined dummy vector to avoid segfaults and the like
	MPI_Isend(&dummyVector[0], 1, MPI_UNSIGNED, root, 53, MPI_COMM_WORLD, &locSignalReq); //tag 53: "signal end of answer entries"

	//work of non-root processors ends here.

	if (rank == root) {

		/*-------------------------------------------------*/
		/*---- receive answer entries from root itself ----*/

		MPI_Status status;		
		while (status.MPI_TAG != 53) { //tag 53: "signal end of answer entries"
			vector<unsigned int> recvEntry(joinArity);
			//blocking receive, so that we continue only when all non-root processors sent "end of answer entries" tag
			MPI_Recv(&recvEntry[0], joinArity, MPI_UNSIGNED, root, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			if (status.MPI_TAG == 43) { //tag 43: "data=answer entry"
				output.addEntry(recvEntry);
			}
		}
	}

	if (rank == root) {
		free(sendentryReqs);
		free(sendentryReqsp);
		delete rel;
		delete relp;
	}

	return output;
}
