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
	//51: signals end of rel entries
	//52: signals end of relp entries
	//13: message contains an answer entry
	//10: signals end of answer entries (from processor status.MPI_SOURCE)
	//(unused) 20: signals end of MPIjoin (all answers have been received by root)

	//main phases:
	//- initialize stuff (only for root)
	//- root Isends rel entries one by one to chosen processors, and each processor Recvs rel entries
	//- once all rel entries have been sent/received, root Isends everyone a message with dummy content and tag 51 (signals end of rel entries)
	//- root Isends relp entries one by one to chosen processors, and each processor Recv relp entries
	//- once all relp entries have been sent/received, root Isends everyone a message with dummy content and tag 52 (signals end of relp entries)
	//- each processor joins received rel and relp entries
	//- each processor Isends back result entries one by one, and Isends to root a message with dummy content and tag 10 (signals end of answer entries)
	//- root (once it has Isent its result entries and signal message to itself) Recvs from all processors answer entries and signal messages
	//- root keeps track of which processor terminated and which did not (using status.MPI_SOURCE)

	//we could not use Bcast to broadcast "signal end of rel/relp" because of the way MPI works: you don't Recv a Bcast, everyone has to call Bcast
	//but what we need is a way to send the signal to the processors, even as they are ready to receive new entries

	cout << "Starting MPIjoin(...). We assume MPI was initialized by caller (MPI_Init(...))." << endl;

	int rank, numtasks;
	const int root = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	//local variables
	MPI_Status localRecvStatus;
	Relation localRel(z.size());
	Relation localRelp(zp.size());
	
	//root-specific variables
	//use pointers because these need to be declared outside "if" to have correct scope, in the point of view of root processor
	Relation *rel;
	Relation *relp;
	Relation *output;
	MPI_Request sendentryReqs[];
	MPI_Request sendentryReqsp[];
	int *k, *kp, *c;	

	if (rank == root) {
		/*----------------------------------------*/
		/*---- setup variables needed by root ----*/
		rel = new Relation(filename, z.size());
		relp = new Relation(filenamep, zp.size());
		rel->setVariables(z);
		relp->setVariables(zp);

		//define k, kp and c (declared outside "if")
		*k = -1; //choose one variable v in the intersection of lists of variables, v = z[k] = zp[kp]
		*c = 0; //number of common variables, to know what arity output will be
		for (int i=0; i<z.size(); i++) {
			for (int j=0; j<zp.size(); j++) {
				if (z[i] == zp[j]) {
					if (*k!=-1) { //if not already set
						*k = i;
						*kp = j;
					}
					(*c)++;
				}
			}
		}

		/*--------------------------*/
		/*---- send rel entries ----*/

		//Nonblocking calls allocate a communication request object and associate it with the request handle (the argument request).
		//The request can be used later to query the status of the communication or wait for its completion. 
		sendentryReqs = malloc(rel->getSize()); //C-style but no choice
		int n=0;

		for (vector<vector<unsigned int> >::iterator it=rel->getBegin(); it!=rel->getEnd(); it++) {
			int m = (*it)[k] % numtasks;
			unsigned int *toSend = rel->getEntries()[0]; //convert vector to array
			MPI_Isend(toSend, rel->getArity(), MPI_UNSIGNED, m, 41, MPI_COMM_WORLD, &sendentryReqs[n]); //tag 41: data=entry for rel
			n++;
		}
	}

	/*--------------------------*/
	/*---- recv rel entries ----*/

	//this is blocking for root!!
	
	localRecvStatus.MPI_TAG = 1;

	while (status.MPI_TAG != 51) { //tag 51: "signal end of rel entries"
		vector<unsigned int> recvEntry(z.size());
		//standard-mode blocking receive
		MPI_Recv(&recvEntry[0], z.size(), MPI_UNSIGNED, partner, MPI_ANY_TAG, MPI_COMM_WORLD, &status);


	}



	if (rank == root) {
		//wait to make sure every processor receives signal "end for rel" in correct order
		MPI_Waitall(rel->getSize(), sendentryReqs, MPI_STATUSES_IGNORE);

		vector<unsigned int> dummyVector(z.size(), 42); //define a well-defined dummy vector to avoid segfaults and the like
		MPI_Request sendsignalReqs[numtasks];
		for (int m=0; m<numtasks; m++) {
			MPI_Isend(dummyVector, z.size(), MPI_UNSIGNED, m, 51, MPI_COMM_WORLD, &sendsignalReqs[m]); //5th arg = "signal end for rel" tag
		}

		//wait to make sure every processor received the signal before starting relp entries
		MPI_Waitall(rel->getSize(), sendnbReqs, MPI_STATUSES_IGNORE);


		/*---------------------------*/
		/*---- send relp entries ----*/

		sendentryReqsp = malloc(relp->getSize());
		int n=0;
		for (vector<vector<unsigned int> >::iterator it=relp->getBegin(); it!=relp->getEnd(); it++) {
			int m = (*it)[kp] % numtasks;
			unsigned int *toSend = relp->getEntries()[0]; //convert vector to array
			MPI_Isend(toSend, relp->getArity(), MPI_UNSIGNED, m, 42, MPI_COMM_WORLD, &sendentryReqsp[n]); //5th arg = "data=entry for relp" tag
			n++;
		}

		MPI_Waitall(rel->getSize(), sendentryReqs, MPI_STATUSES_IGNORE);

		MPI_Request sendnbReqsp[numtasks];
		for (int m=0; m<numtasks; m++) {
			MPI_Isend(nbToSendp[m], 1, MPI_UNSIGNED, m, 52, MPI_COMM_WORLD, &sendnbReqsp[m]); //5th arg = "signal end for relp" tag
		}

	}

	/*---------------------------------------*/
	/*---- recv and process relp entries ----*/










	if (rank == root) {
		//wait to make sure every processor receives signal "end for relp" in correct order
		MPI_Waitall(relp->getSize(), sendentryReqsp, MPI_STATUSES_IGNORE);
		
		
		sendentryReqs = malloc(rel->getSize());
		
		vector<unsigned int> dummyVectorp(zp.size(), 42);
		for (int m=0; m<numtasks; m++) {
			MPI_Isend(dummyVectorp, zp.size(), MPI_UNSIGNED, m, 52, MPI_COMM_WORLD, &sendsignalReqs[m]); //5th arg = "signal end for rel" tag
		}

		//wait to make sure every processor received the signal before starting relp entries
		MPI_Waitall(rel->getSize(), sendnbReqs, MPI_STATUSES_IGNORE);
	
	}

	if (rank == root) {
		free(sendentryReqs);
		free(sendentryReqsp);
		delete rel;
		delete relp;
	}

	return Relation(1);
}
