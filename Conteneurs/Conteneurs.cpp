#include "Conteneurs.hpp"
#include <iostream>

ConteneurNaif::ConteneurNaif() {
}

ConteneurNaif::ConteneurNaif(const vector<vector<int> >& data) : _data(data) {

}

int& ConteneurNaif::get(int x, int y) {
	return this->_data[x][y];
}

int ConteneurNaif::size() const {
	return _data.size() * ((_data.empty()) ? 0 : _data[0].size());
}

ConteneurBogoss::ConteneurBogoss() {
	_width = 0;
	_height = 0;
}

ConteneurBogoss::ConteneurBogoss(const vector<vector<int> >& data) : _height(data.size()), _width( (data.empty()) ? 0 : data[0].size() ) {
	_data.resize(_width * _height);
	for (int x = 0; x < _height; ++x)
		for (int y = 0; y < _width; ++y)
			_data[x * _width + y] = data[x][y];
}

int& ConteneurBogoss::get(int x, int y) {
	if (x * _width + y >= _data.size())
		cerr << "_data.size :" << _data.size() << "\nx : " << x << "\ny : " << y << "\nx * _width + y : " << x * _width + y << endl;
	return _data[x * _width + y];
}

int ConteneurBogoss::size() const {
	return _data.size();
}