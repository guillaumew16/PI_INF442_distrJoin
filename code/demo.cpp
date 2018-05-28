#include "relation.hpp"
#include <iostream>
#include <string>

using namespace std;

int main(int argc, char** argv) {
    cout << "Choose among: triangle" << endl;
    string cmd;
    cin >> cmd;

    //apparently using switch with strings is not recommended
    if (cmd == "triangle") {
        cout << "data file: ";
        string filePath;
        cin >> filePath;
        Relation rel(filePath.c_str(), 2);
        triangle(rel);
    } else {
        cout << "Unrecognized cmd. Abort" << endl;
    }

    return 0;
}