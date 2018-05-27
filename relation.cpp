#include <iostream>
#include <fstream>
#include <algorithm> // std::sort

#include <unistd.h> //sleep

#include "relation.hpp"
//#include "permutation.hpp" <-- already done

using namespace std;

Relation::Relation(int r) {
	this->r = r;
}

Relation::Relation(const char *filename) {
	cout << "Importing relation from file " << filename << ", assuming it is in correct format and has arity 2." << endl;
	cout << "To import a non-binary relation from a file, we will need to modify Relation's constructor." << endl;

	// assuming relation described in the file is binary
	this->r = 2;

	ifstream file(filename);
	unsigned int a, b;
	while (file >> a >> b) {
		entries.push_back(vector<unsigned int>(2));
		entries.back()[0] = a;
		entries.back()[1] = b;
	}
	file.close();
}

// Relation::~Relation() {} //use the default destructor, which (recursively) calls destructor on each member

void Relation::addEntry(vector<unsigned int> newEntry) {
	if (newEntry.size() != this->r) {
		throw invalid_argument("received newEntry of size != r");
	}
	entries.push_back(newEntry);
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

int Relation::getArity() const {
	return r;
}

void Relation::head(int nl) {
	if (entries.empty()){
		cout << "no entries" << endl;
		return;
	}
	int n=0;
	for (vector<vector<unsigned int> >::iterator it=entries.begin(); it!=entries.end(); it++) {
		n++;
		cout << "entry #" << n << ": " << (*it)[0] <<" "<< (*it)[1] << endl;
		if (n == nl) { //we print all entries if nl<0
			return;
		}
	}
}

void Relation::writeToFile(const char *filename)
{
	cout << "Calling Relation::writeToFile(...) but it is not finished testing: must test support of non-binary Relations" << endl;
	cout << "Writing relation to file " << filename << ", assuming it has arity 2." << endl;
	cout << "To export a non-binary relation to a file, we will need to modify Relation::writeToFile(...)." << endl;

	ofstream file(filename);
	for (vector<vector<unsigned int>>::iterator it=entries.begin(); it!=entries.end(); it++) {
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

Relation join(Relation rel, vector<int> z, Relation relp, vector<int> zp){

	cout << "called join(...), but not finished testing" << endl;
	cout << "Computing join..." << endl;

	if (rel.getArity() != z.size())
		throw invalid_argument("First relation (rel) has arity != size of first list of variables (z)");
	if (relp.getArity() != zp.size())
		throw invalid_argument("Second relation (relp) has arity != size of second list of variables (zp)");

	//assuming there exists a global indexation of variables, v_j, z[i] is defined as: for given assignment a,
	//rel.getEntries()[a][i] = a(v_{z[i]})
	//idem for relp/zp. no assumption on order of z nor zp.

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
	//we sill build permut and permutp s.t 
	//x (ordered set) = (z_{permut[0]}, ..., z_{permut[c-1]}) and (zp_{permutp[0]}, ..., zp_{permutp[c-1]})


	//will be useful to fill the rest of permut and permutp
	vector<bool> rest(rel.getArity(), true);
	vector<bool> restp(relp.getArity(), true);

	for (int i=0; i<z.size(); i++) {
		for (int j=0; j<zp.size(); j++) {
			if (z[i] == zp[j]){
				x.push_back(z[i]);
				permutVect.push_back(i);
				permutpVect.push_back(j);
				rest[i] = false;
				restp[j] = false;
				break;
			}
		}
	}

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
	
	/**/
	//intermediary outputting for testing
	rel.writeToFile("output/rel.txt");
	relp.writeToFile("output/relp.txt");
	printVector(x, "x");
	printVector(permut.getPermut(), "permut");
	printVector(permutp.getPermut(), "permutp");
	/**/

	/*-----------------------------------------------------------------------*/
	/***** iterate over both Relations and add tuples that coincide on x *****/
	cout << "| iterating over both Relations and adding tuples that coincide on x..." << endl;

	vector<vector<unsigned int> >::iterator t_it = rel.getBegin();
	vector<vector<unsigned int> >::iterator tp_it = relp.getBegin();
	
	Relation output(rel.getArity() + relp.getArity()); //mergeEntry is just concatenate entries

	int n=0;
	while (t_it != rel.getEnd() && tp_it != relp.getEnd()) {
		n++;
		cout << "|  | iteration " << n << ": ";
		if (coincide(*t_it, permut, *tp_it, permutp, x.size())) { //this makes sense because x is ordered ;)
 			//"t and tp coincide on x"
			cout << "t and tp coincide on x" << endl;

			//find where to stop (equivalent to a vector::end())
			vector<vector<unsigned int> >::iterator s_it = t_it;
			vector<vector<unsigned int> >::iterator sp_it = tp_it;
			while (s_it!=rel.getEnd() && agree(*s_it, *t_it, permut, x.size())) {
				//"s and t agree on x"
				s_it++;
			}
			while (sp_it!=relp.getEnd() && agree(*sp_it, *tp_it, permutp, x.size())) {
				sp_it++;
			}
			
			//add matching entries by iterating over both relations
			for (vector<vector<unsigned int> >::iterator it=t_it; it!=s_it; it++) {
				for (vector<vector<unsigned int> >::iterator itp=tp_it; itp!=sp_it; itp++) {
					output.addEntry(mergeEntry(*it, permut, *itp, permutp, x.size()));
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
		output[i] = t[permut.getPermut()[i]];
	}
	return output;
}

bool coincide(vector<unsigned int> t, Permutation permut, vector<unsigned int> tp, Permutation permutp, int c) {
	//"t and tp coincide on x"
	//where x (ordered set) is represented by (permut[0], ..., permut[c-1]) and (permutp[0], ..., permutp[c-1])
	//this is equivalent to (pi_x(t) >= pi_x(tp)) && (pi_x(tp) >= pi_x(t))
	// !lexicoCompare(pi_x(*t_it, permut, c), pi_x(*tp_it, permutp, c)) && !lexicoCompare(pi_x(*tp_it, permutp, c), pi_x(*t_it, permut, c))

	//project on x
	vector<unsigned int> t_proj = pi_x(t, permut, c);
	vector<unsigned int> tp_proj = pi_x(tp, permutp, c);

	bool shouldReturn = (!lexicoCompare(t_proj, tp_proj) ) && (!lexicoCompare(tp_proj, t_proj) ); //for testing purposes

	for (int i=0; i<t_proj.size(); i++) {
		if (t_proj[i] != tp_proj[i]) {
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
		if (s[permut.getPermut()[i]] != t[permut.getPermut()[i]]) {
			return false;
		}
	}
	return true;
}

vector<unsigned int> mergeEntry(vector<unsigned int> t, Permutation permut, vector<unsigned int> tp, Permutation permutp, int c) {
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
