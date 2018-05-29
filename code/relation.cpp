#include <iostream>
#include <fstream>
#include <algorithm> // std::sort

#include "relation.hpp"
//#include "permutation.hpp" <-- already done

using namespace std;

Relation::Relation(int r) {
	this->r = r;
	for (int i=0; i<r; i++) { //z defaults to identity
		z[i] = i;
	}
}

Relation::Relation(const char *filename, int r) {
	//"won't-fix" bug: 
	//chez moi quand on donne un filename qui n'existe pas, le constructor de ifstream ne renvoie pas d'exception et on obtient une Relation vide.

	cout << "Importing relation from file " << filename << ", *assuming* it is in correct format and has specified arity (" << r << ")" << endl;

	this->r = r;
	for (int i=0; i<r; i++) { //z defaults to identity
		z[i] = i;
	}

	ifstream file(filename);
	unsigned int newValue = -1;
	int n=0;
	while (file >> newValue) {
		if (n == 0) {
			entries.push_back(vector<unsigned int>(r));
		}
		entries.back()[n] = newValue;

		n++;
		if (n == r) { //we could have used n%r instead, but file may be very long, so this is easier
			n = 0;
		}
	}
	
	/* for record, the old code for binary relations only:
	unsigned int a, b;
	while (file >> a >> b) {
		entries.push_back(vector<unsigned int>(2));
		entries.back()[0] = a;
		entries.back()[1] = b;
	}
	*/

	file.close();
}

// Relation::~Relation() {} //use the default destructor, which (recursively) calls destructor on each member

int Relation::getArity() const {
	return r;
}

vector<int> getVariables() const {
	return z;
}

int getVariable(int i) const {
	if (i >= r)
		throw invalid_argument("tried to getVariable for an index >= r");
	return z[i];
}

void setVariables(vector<int> newZ) {
	if (newZ.size() != this->r)
		throw invalid_argument("tried to set z (list of variables) to a vector of size != r");

	for (int i=0; i<r; i++) {
		z[i] = newZ[i];
	}
}

vector<vector<unsigned int>> Relation::getEntries() const {
	return entries;
}

vector<vector<unsigned int> >::iterator Relation::getBegin() {
	return entries.begin();
}

vector<vector<unsigned int> >::iterator Relation::getEnd() {
	return entries.end();
}

void Relation::addEntry(vector<unsigned int> newEntry) {
	if (newEntry.size() != this->r)
		throw invalid_argument("received newEntry of size != r");

	entries.push_back(newEntry);
}

void Relation::head(int nl) {
	if (entries.empty()){
		cout << "no entries" << endl;
		return;
	}
	int n=0;
	for (vector<vector<unsigned int> >::iterator it=entries.begin(); it!=entries.end(); it++) {
		n++;
		cout << "entry #" << n << ": ";
		for (int i=0; i < r-1; i++) {
			cout << (*it)[i] << " ";
		}
		cout << (*it)[r-1] << endl;

		if (n == nl) { //we print all entries if nl<0
			return;
		}
	}
}

void Relation::writeToFile(const char *filename)
{
	cout << "Writing relation to file " << filename << endl;

	ofstream file(filename);
	for (vector<vector<unsigned int> >::iterator it=entries.begin(); it!=entries.end(); it++) {
		for (int i=0; i < r-1; i++) {
			file << (*it)[i] << " ";
		}
		file << (*it)[r-1] << endl;
	}

	file.close();
}

void Relation::lexicoSort(Permutation permut) {
	if (permut.getDimension() != this->r)
		throw invalid_argument("received permutation of dimension != r");
	
	sort(entries.begin(), entries.end(), 
		[permut](vector<unsigned int> const &a, vector<unsigned int> const &b) { return lexicoCompare(permut.permute(a), permut.permute(b)); });

	/*alternatively we could have done "radix sort" using stable_sort. slightly less efficient due to stable_sort I think
	for (int i=0; i<r; i++){
		stable_sort(entries.begin(), entries.end(),
			 [i, permut](vector<unsigned int> const &a, vector<unsigned int> const &b) { return a[permut[i]] < b[permut[i]]; });
	}*/
}

bool lexicoCompare(vector<unsigned int> e1, vector<unsigned int> e2) {
	if (e1.size() != e2.size())
		throw invalid_argument("tried to compare entries of different dimensions");
	
	for (int i=0; i<e1.size(); i++) {
		if (e1[i] < e2[i]) {
			return true;
		} else if (e1[i] > e2[i]) {
			return false;
		}
	}
	return false;
}

Relation join(Relation rel, Relation relp) {

	cout << "called join(...), but not finished testing: must test after adding z (list of variables) as attribute of Relation" << endl;
	cout << "Computing join..." << endl;

	//NB: assuming there exists a global indexation of variables, v_z, rel.z[i] is defined as: for given assignment (of variables) a,
	//rel.getEntries()[a][i] = a(v_{z[i]})
	//in the comments for this function, we may note zp=relp.z

	/*---------------------------------------------------------------------------*/
	/***** get set of common variables and their representations in z and zp *****/
	cout << "| getting set of common variables (x, permut, permutp)..." << endl;
	cout << "|  | building x and x.size() first values of permutVect and permutpVect... ";

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

	cout << "finished" << endl;
	cout << "|  | filling the rest of permutVect and permutpVect... ";

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

	rel.lexicoSort(permut);
	relp.lexicoSort(permutp);

	cout << "finished" << endl;
	
	/*
	//intermediary outputting for testing
	rel.writeToFile("output/rel.txt");
	relp.writeToFile("output/relp.txt");
	printVector(x, "x");
	printVector(permut.getPermut(), "permut");
	printVector(permutp.getPermut(), "permutp");
	*/

	/*-----------------------------------------------------------------------*/
	/***** iterate over both Relations and add tuples that coincide on x *****/
	cout << "| iterating over both Relations and adding tuples that coincide on x..." << endl;

	//TODO: begin work (for adapting code after putting z as attribute of rel) --v
	//EDIT: to check, but I think this is ok. just need to modify mergeEntry(...), and re-check whole function

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
	//the way we set list of variables here must be consistent with how we do mergeEntry(...)!!

	int n=0;
	while (t_it != rel.getEnd() && tp_it != relp.getEnd()) {
		n++;
		cout << "|  | iteration " << n << ": ";
		if (coincide(*t_it, permut, *tp_it, permutp, c)) {
 			//"t and tp coincide on x"
			cout << "t and tp coincide on x" << endl;

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
					cout << "|  |  | nb of entries in output so far: " << output.getEntries().size() << endl;
				}
			}

			//"jump in rel and relp to the first tuples that disagree with t and tp"
			t_it = s_it;
			tp_it = sp_it;

		} else if ( lexicoCompare(pi_x(*t_it, permut, x.size()), pi_x(*tp_it, permutp, x.size())) ) {
			//pi_x(t) < pi_x(tp) "pi_x(t) is lexicographically (strictly) smaller than pi_x(tp)"
			//	"go to the next tuple t"
			cout << "pi_x(t) < pi_x(tp). jump to the next tuple t" << endl;

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
			cout << "pi_x(t) > pi_x(tp). jump to the next tuple tp" << endl;

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

	cout << "| finished building join Relation." << endl;
	return output;
}

vector<unsigned int> pi_x(vector<unsigned int> t, Permutation permut, int c) {
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

bool coincide(vector<unsigned int> t, Permutation permut, vector<unsigned int> tp, Permutation permutp, int c) {
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

bool agree(vector<unsigned int> s, vector<unsigned int> t, Permutation permut, int c) {
	//"entries s and t (both from Relation rel) agree on the variables { permut[0], ..., permut[c-1] } (= x)"

	if (s.size() != t.size())
		throw invalid_argument("tried to test if s and t agree (on c first variables of permut), but they have different dimensions");
	if (c > permut.getDimension())
		throw invalid_argument("tried to test if s and t agree on c first variables of permut, but permut.size() < c");
	if (permut.getDimension() != s.size())
		throw invalid_argument("called agree(...) but permut has dimension != dimension of entries. Should not have happened...");

	for (int i=0; i<c; i++) {
		if (s[permut.getPermut(i)] != t[permut.getPermut(i) {
			return false;
		}
	}
	return true;
}

vector<unsigned int> mergeEntry(vector<unsigned int> t, Permutation permut, vector<unsigned int> tp, Permutation permutp, int c) {

	//TODO: begin work on mergeEntry, to match our choice of list of variables for join(...)'s output --v
	cout << "called mergeEntry, but not finished coding: must recode because we put z as Relation attribute" << endl;

	//merge t (from Relation rel) and tp (from Relation relp) into a single entry of join(rel,relp)
	//here we simply concatenate the two entries
	//(whereas normally we would try to avoid repeating common variables)

	//sanity check that t and tp agree on the (ordered) set x of common variables
	//where x (ordered set) is represented by (permut[0], ..., permut[c-1]) and (permutp[0], ..., permutp[c-1])
	if (!coincide(t, permut, tp, permutp, c))
		throw invalid_argument("tried to mergeEntry two entries which do not coincide on x");

	//simply concatenate
	vector<unsigned int> output(t.size() + tp.size());
	for (int i=0; i<t.size(); i++) {
		output[i] = t[i];
	}
	for (int i=0; i<tp.size(); i++) {
		output[t.size() + i] = tp[i];
	}
	return output;
}

Relation triangle(Relation rel) {
    cout << "Computing triangles of graph-relation..." << endl;
    if (rel.getArity() != 2) {
        cout << "| Error: input relation has arity != 2. Abort" << endl;
    }
    
    //it doesn't matter that z[i] is not in range [0, 2] (we assume global indexation of variables v_j)
	vector<int> z12(2);
	z12[0]=1;
	z12[1]=2;
	vector<int> z23(2);
	z23[0]=2;
	z23[1]=3;
    vector<int> zInterm(4);
    zInterm[0]=1;
    zInterm[1]=2;
    zInterm[2]=2;
    zInterm[3]=3;
	vector<int> z13(2);
	z13[0]=1;
	z13[1]=3;

    Relation intermRel = join(rel, z12, rel, z23);
    //intermRel.writeToFile("../output/triangle-intermediary.txt");
    Relation triangle = join(intermRel, zInterm, rel, z13);

    return triangle;
}

void printVector(vector<int> v, const char *name) {
	cout << "Printing vector<int> " << name << ": " << endl;
	for (vector<int>::iterator it=v.begin(); it!=v.end(); it++){
		cout << *it << ' ';
	}
	cout << endl;
}

void printVector(vector<unsigned int> v, const char *name) {
	cout << "Printing vector<unsigned int> " << name << ": " << endl;
	for (vector<unsigned int>::iterator it=v.begin(); it!=v.end(); it++){
		cout << *it << ' ';
	}
	cout << endl;
}
