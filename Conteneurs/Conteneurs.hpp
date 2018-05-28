#pragma once

#include <vector>

using namespace std;

class ConteneurNaif {
public :
	ConteneurNaif();
	ConteneurNaif(const vector<vector<int> >& data);
	int& get(int x, int y);
	
	int size() const;

private:
	vector<vector<int> > _data;


};

class ConteneurBogoss {
public:
	ConteneurBogoss();
	ConteneurBogoss(const vector<vector<int> >& data);
	int& get(int x, int y);

	int size() const;

private:
	vector<int> _data;
	int _width, _height;
};