#include <iostream>
#include <string>
#include "flexql.h"

int callback(void *data, int columnCount, char **values, char **columnNames) {
    for (int i = 0; i < columnCount; i++) {
        std::cout << columnNames[i] << " = " << (values[i] ? values[i] : "NULL") << "\n";
    }
    std::cout << "\n"; 
    return 0; 
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 9000;

    if (argc >= 3) {
        host = argv[1];
        port = std::stoi(argv[2]);
    }

    FlexQL *db;
    char *errMsg = nullptr;

    if (flexql_open(host.c_str(), port, &db) != FLEXQL_OK) {
        std::cerr << "Cannot connect to FlexQL server\n";
        return 1;
    }
    std::cout << "Connected to FlexQL server\n\n";

    std::string input_line;
    while (true) {
        std::cout << "flexql> ";
        
        if (!std::getline(std::cin, input_line)) break;
        if (input_line == ".exit") break;
        if (input_line.empty()) continue;

        if (flexql_exec(db, input_line.c_str(), callback, nullptr, &errMsg) != FLEXQL_OK) {
            std::cout << "SQL ERROR: " << errMsg << "\n";
            flexql_free(errMsg);
        }
    }

    std::cout << "Connection closed\n";
    flexql_close(db);
    return 0;
}