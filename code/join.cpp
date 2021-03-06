#include <iostream>
#include <fstream>
#include <algorithm> // std::sort
#include <stdexcept>

#include "join.hpp"
//#include "relation.hpp" <-- already done
//#include "permutation.hpp" <-- already done

using namespace std;

Relation join(Relation &rel, Relation &relp, int verbose) { //verbose = 0(quiet) or 1(verbose) or 2(very verbose). parameter default: verbose=1.
	//the only side effect on rel and relp is calling lexicoSort, so we reorder the entries but underlying database is unchanged
	if (verbose>=1) cout << "Computing join..." << endl;

	//NB: assuming there exists a global indexation of variables, v_z, rel.z[i] is defined as: for given assignment (of variables) a,
	//rel.getEntry(a)[i] = a(v_{z[i]})
	//in the comments for this function, we may note zp=relp.z

	/*---------------------------------------------------------------------------*/
	/*---- get set of common variables and their representations in z and zp ----*/
	if (verbose>=1) cout << "| getting set of common variables (x, permut, permutp)..." << endl;
	if (verbose>=1) cout << "|  | building x and x.size() first values of permutVect and permutpVect... ";

	vector<int> x; 
	//NB: we consider x, the intersection of z and zp, to be an ordered set of variables
	//the order is arbitrary (it is the one that is obtained by iterating through z and then through zp (see infra))
	//but it is fixed for the rest of the join algorithm.
	vector<int> permutVect;
	permutVect.reserve(rel.getArity());
	vector<int> permutpVect;
	permutpVect.reserve(relp.getArity());
	//NB: we will build permut and permutp s.t 
	//x (ordered set) = (z[permut[0]], ..., z[permut[c-1]])
	//                = (zp[permutp[0]], ..., zp[permutp[c-1]])
	//in other words, if we permut.permute( entries of rel ), we obtain x's variables first:
	//for an entry (before permutation)  (a(v_z0), a(v_z1), ....., a(v_z[r-1]))
	//                    we will obtain (a(v_x0), ..., a(v_x[c-1]), .........)
	//where a is the assignment (of variables) corresponding to the entry, and c=x.size()

	//will be useful to fill the rest of permut and permutp
	vector<bool> rest(rel.getArity(), true);
	vector<bool> restp(relp.getArity(), true);

	for (int i=0; i<rel.getArity(); i++) {
		for (int j=0; j<relp.getArity(); j++) {
			if (rel.getVariable(i) == relp.getVariable(j)){ //z[i] == zp[j]
				x.push_back(rel.getVariable(i));
				permutVect.push_back(i);
				permutpVect.push_back(j);
				rest[i] = false;
				restp[j] = false;
				break;
			}
		}
	}
	//"proof" of claim on permut: at the n-th time that z[i]==zp[j] was verified,
	//x[n] = z[i]
	//permut[n] = i
	//and thus x[n] = z[permut[n]]
	//(idem for relp/zp/permutp of course)

	if (verbose >= 1) cout << "finished" << endl;
	if (verbose >= 1) cout << "|  | filling the rest of permutVect and permutpVect... ";

	for (int i=0; i<rest.size(); i++) {
		if (rest[i]) {
			permutVect.push_back(i);
		}
	}
	for (int i=0; i<restp.size(); i++) {
		if (restp[i]) {
			permutpVect.push_back(i);
		}
	}
	if (permutVect.size() != rel.getArity())
		throw length_error("size of permutVect after filling with rest does not match arity of rel");
	if (permutpVect.size() != relp.getArity())
		throw length_error("size of permutpVect after filling with restp does not match arity of relp");

	Permutation permut(permutVect);
	Permutation permutp(permutpVect);

	rel.lexicoSort(permut); //reorders entries, but underlying database is unchanged
	relp.lexicoSort(permutp);

	if (verbose>=1) cout << "finished" << endl;
	
	/*
	//intermediary outputting for testing
	rel.writeToFile("output/rel.txt");
	relp.writeToFile("output/relp.txt");
	printVector(x, "x");
	printVector(permut.getPermut(), "permut");
	printVector(permutp.getPermut(), "permutp");
	*/

	/*-----------------------------------------------------------------------*/
	/*---- iterate over both Relations and add tuples that coincide on x ----*/
	if (verbose>=1) cout << "| iterating over both Relations and adding tuples that coincide on x..." << endl;

	vector<vector<unsigned int> >::iterator t_it = rel.getBegin();
	vector<vector<unsigned int> >::iterator tp_it = relp.getBegin();
	
	int c = x.size(); //turns out it is very often used
	//many auxiliary functions take parameters (t, permut, c), to allow extraction of variables in x from entry t
	Relation output(rel.getArity() + relp.getArity() - c);

	vector<int> newZ = permut.permute(rel.getVariables());
	for (int i=c; i<relp.getArity(); i++) {
		newZ.push_back(relp.getVariable(permutp.getPermut(i)));
	}
	output.setVariables(newZ); 
	//we (arbitrarily) chose to set list of variables of join relation to be
	//the permut.permuted rel.getVariables(), followed by the part of the permutp.permuted relp.getVariables() that is not in x
	//i.e. (v_x0, ..., v_x{c-1}, [rest of variables of rel in permut'ed order], [rest of variables of relp in permutp'ed order])
	//NB: the way we set list of variables here must be consistent with how we do mergeEntry(...)!!

	int n=0;
	while (t_it != rel.getEnd() && tp_it != relp.getEnd()) {
		n++;
		if (verbose>=2) cout << "|  | iteration " << n << ": ";
		if (coincide(*t_it, permut, *tp_it, permutp, c)) {
 			//"t and tp coincide on x"
			if (verbose>=2) cout << "t and tp coincide on x" << endl;

			//find where to stop (equivalent to a vector::end())
			vector<vector<unsigned int> >::iterator s_it = t_it;
			vector<vector<unsigned int> >::iterator sp_it = tp_it;
			while (s_it!=rel.getEnd() && agree(*s_it, *t_it, permut, c)) {
				//"s and t agree on x"
				s_it++;
			}
			while (sp_it!=relp.getEnd() && agree(*sp_it, *tp_it, permutp, c)) {
				sp_it++;
			}
			
			//add matching entries by iterating over both relations
			for (vector<vector<unsigned int> >::iterator it=t_it; it!=s_it; it++) {
				for (vector<vector<unsigned int> >::iterator itp=tp_it; itp!=sp_it; itp++) {
					output.addEntry(mergeEntry(*it, permut, *itp, permutp, c));
					if (verbose>=2) cout << "|  |  | nb of entries in output so far: " << output.getEntries().size() << endl;
				}
			}

			//"jump in rel and relp to the first tuples that disagree with t and tp"
			t_it = s_it;
			tp_it = sp_it;

		} else if ( lexicoCompare(pi_x(*t_it, permut, x.size()), pi_x(*tp_it, permutp, x.size())) ) {
			//pi_x(t) < pi_x(tp) "pi_x(t) is lexicographically (strictly) smaller than pi_x(tp)"
			//	"go to the next tuple t"
			if (verbose>=2) cout << "pi_x(t) < pi_x(tp). jump to the next tuple t" << endl;

			//small optimization: jump to the next entry t1 of rel s.t t does not agree with t1 on x
			vector<vector<unsigned int> >::iterator s_it = t_it;
			while (s_it!=rel.getEnd() && agree(*s_it, *t_it, permut, x.size())) {
				//"s and t agree on x"
				s_it++;
			}
			t_it = s_it;

		} else {
			//pi_x(t) > pi_x(tp)
			//	"go to the next tuple tp"
			if (verbose>=2) cout << "pi_x(t) > pi_x(tp). jump to the next tuple tp" << endl;

			vector<vector<unsigned int> >::iterator sp_it = tp_it;
			while (sp_it!=relp.getEnd() && agree(*sp_it, *tp_it, permutp, x.size())) {
				//"s and t agree on x"
				sp_it++;
			}
			tp_it = sp_it;
			/*
			if (tp_it == relp.getEnd()){
				cout << "tp is now at the end of relp ~" << endl;
			}
			*/
		}
	}

	if (verbose>=1) cout << "| finished building join Relation." << endl;
	return output;
}

vector<unsigned int> pi_x(vector<unsigned int> &t, Permutation &permut, int c) {
	//if v_j is the global indexation, and x is the (ordered) set of variables on which to join,
	//	c is cardinality of x
	//	permut is the permutation s.t x = ( z_{permut[0]}, ..., z_{permut[c-1]} )
	//		where z denotes the variable list of the Relation containing t
	//	t is the entry to project onto x
	//we output t_proj s.t t_proj[i] = t_{x_i} = t[permut[i]]

	//NB: this is NOT equivalent to a Permutation::permute(...)! the output has size only c.
	//but this is equivalent to a permutation followed by truncation

	if (c > permut.getDimension())
		throw invalid_argument("called pi_x to project on c first variables of permut, but permut.dimension() < c");
	if (permut.getDimension() != t.size())
		throw invalid_argument("called pi_x(...) but permut has dimension != dimension of entry to project. Should not have happened...");

	vector<unsigned int> output(c);
	for (int i=0; i<c; i++) {
		output[i] = t[permut.getPermut(i)];
	}
	return output;
}

bool coincide(vector<unsigned int> &t, Permutation &permut, vector<unsigned int> &tp, Permutation &permutp, int c) {
	//"t and tp coincide on x"
	//where x (ordered set) is represented by (permut[0], ..., permut[c-1]) and (permutp[0], ..., permutp[c-1])
	//this is equivalent to (pi_x(t) >= pi_x(tp)) && (pi_x(tp) >= pi_x(t))
	// !lexicoCompare(pi_x(*t_it, permut, c), pi_x(*tp_it, permutp, c)) && !lexicoCompare(pi_x(*tp_it, permutp, c), pi_x(*t_it, permut, c))

	//for testing purposes
	//project on x
	vector<unsigned int> t_proj = pi_x(t, permut, c);
	vector<unsigned int> tp_proj = pi_x(tp, permutp, c);
	bool shouldReturn = (!lexicoCompare(t_proj, tp_proj) ) && (!lexicoCompare(tp_proj, t_proj) );

	for (int i=0; i<c; i++) {
		if (t[permut.getPermut(i)] != tp[permutp.getPermut(i)]) {
			if (shouldReturn != false)
				throw logic_error("coincide(t, permut, tp, permut, c)=false does not return expected value (pi_x(t) >= pi_x(tp)) && (pi_x(tp) >= pi_x(t)) =true");
			return false;
		}
	}

	if (shouldReturn != true)
		throw logic_error("coincide(t, permut, tp, permut, c)=true does not return expected value (pi_x(t) >= pi_x(tp)) && (pi_x(tp) >= pi_x(t)) =false");
	return true;
}

bool agree(vector<unsigned int> &s, vector<unsigned int> &t, Permutation &permut, int c) {
	//"entries s and t (both from Relation rel) agree on the variables { permut[0], ..., permut[c-1] } (= x)"

	if (s.size() != t.size())
		throw invalid_argument("tried to test if s and t agree (on c first variables of permut), but they have different dimensions");
	if (c > permut.getDimension())
		throw invalid_argument("tried to test if s and t agree on c first variables of permut, but permut.size() < c");
	if (permut.getDimension() != s.size())
		throw invalid_argument("called agree(...) but permut has dimension != dimension of entries. Should not have happened...");

	for (int i=0; i<c; i++) {
		if (s[permut.getPermut(i)] != t[permut.getPermut(i)]) {
			return false;
		}
	}
	return true;
}

vector<unsigned int> mergeEntry(vector<unsigned int> &t, Permutation &permut, vector<unsigned int> &tp, Permutation &permutp, int c) {
	//merge t (from Relation rel) and tp (from Relation relp) into a single entry of join(rel,relp)
	
	//we (arbitrarily) chose to set list of variables of join relation to be
	//the permut.permuted rel.getVariables(), followed by the part of the permutp.permuted relp.getVariables() that is not in x
	//i.e. (v_x0, ..., v_x{c-1}, [rest of variables of rel in permut'ed order], [rest of variables of relp in permutp'ed order])
	//NB: the way we do mergeEntry(...) must be consistent with how we set list of variables in join(...)!!

	//sanity check that t and tp agree on the (ordered) set x of common variables
	//where x (ordered set) is represented by (permut[0], ..., permut[c-1]) and (permutp[0], ..., permutp[c-1])
	if (!coincide(t, permut, tp, permutp, c))
		throw invalid_argument("tried to mergeEntry two entries which do not coincide on x");

	vector<unsigned int> output;
	output.reserve(t.size() + tp.size() - c);

	//the variables in t
	for (int i=0; i<t.size(); i++) {
		output.push_back( t[permut.getPermut(i)] );
	}
	//{variables in tp} \ {variables in t}
	for (int i=c; i<tp.size(); i++) {
		output.push_back( tp[permutp.getPermut(i)] );
	}

	return output;
}

Relation autoJoin(Relation &rel, vector<int> &zp) {
	//equivalent to
	/*
	Relation relp(rel);
	relp.setVariables(zp);
	return join(rel, relp);
	*/

	if (rel.getArity() != zp.size())
		throw invalid_argument("called autoJoin for rel with zp, zp has size != arity of rel");

	//looks like a dirty hack, but actually we do need two Relation objects in order to run join(...)
	//because we need to iterate simultaneously over a permut-lexicoSorted Relation and a permutp-lexicoSorted Relation
	//(unless we store the entry permutation for the permutp-sorted one... which we could have done)
	Relation relp(rel);
	relp.setVariables(zp);
	return join(rel, relp);
}

Relation triangle(Relation &rel) {
    cout << "Computing triangles of graph-relation..." << endl;
	if (rel.getArity() != 2)
		throw invalid_argument("called MPItriangle on input relation with arity != 2");

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
    Relation intermRel = autoJoin(rel, z23);
/*
	intermRel.writeToFile("../output/triangle-intermediary.txt");
	Permutation identity(intermRel.getArity());
	intermRel.lexicoSort(identity);
	intermRel.writeToFile("../output/triangle-intermediary_sorted.txt");
*/
	rel.setVariables(z13);
    Relation triangle = join(intermRel, rel);

	//format triangle to avoid repeating entries
	triangle.formatTriangle();

    return triangle;
}

Relation multiJoin(vector<Relation> toJoin) {
	//joins toJoin's elements in the order in which they appear in toJoin:
	// (((toJoin[0] >< toJoin[1]) >< toJoin[2]) >< ...)
	if (toJoin.size() == 0)
		throw invalid_argument("called multiJoin on empty parameter toJoin");

	cout << "Computing multiJoin. Make sure you setVariables on input before call!" << endl;

	Relation output(toJoin[0]);
	for (int i=1; i<toJoin.size(); i++) {
		output = join(output, toJoin[i], 0); //0 for non-verbose
	}

	return output;
}