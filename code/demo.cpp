#include "relation.hpp"
#include <iostream>
#include <string>

using namespace std;

int main(int argc, char** argv) {
    string cmd;
    cout << "Choose among: triangle" << endl;
    cin >> cmd;

    //apparently using switch with strings is not recommended
    if (cmd == "join") {
        string filePath1;
        string filePath2;
        int r1, r2;
        cout << "data file for first relation: " << endl;
        cin >> filePath1;
        cout << "arity of first relation: " << endl;
        cin >> r1;
        cout << "data file for second relation (or leave empty to use the first relation): " << endl;
        cin >> filePath2;
        cout << "arity of second relation: " << endl;
        cin >> r2;
        if (filePath2 == "") {
            filePath2 = filePath1;
        }
        Relation rel1(filePath1.c_str(), r1);
        Relation rel2(filePath2.c_str(), r2);
        Relation rel = join(rel1, rel2);
        rel.writeToFile("../output/join.txt");

    } else if (cmd == "triangle") {
        string filePath;
        cout << "data file for graph-relation: (no verification that input is indeed a binary relation)" <<endl;
        cin >> filePath;
        Relation rel(filePath.c_str(), 2);
        Relation triRel = triangle(rel);
        triRel.writeToFile("../output/triangle.txt");

    } else {
        cout << "Unrecognized cmd. Abort" << endl;
    }

    return 0;
}