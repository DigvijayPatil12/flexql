#include "flexql.h"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fstream>

// Opaque connection handle exposing only the socket descriptor to the client
struct FlexQL {
    int sockfd;
};


// 1. CONNECTION MANAGEMENT
// Initializes a TCP socket and establishes a synchronous connection to the FlexQL server
int flexql_open(const char *host, int port, FlexQL **db) {
    int sock = 0;
    struct sockaddr_in serv_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return FLEXQL_ERROR;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        return FLEXQL_ERROR;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return FLEXQL_ERROR;
    }

    *db = (FlexQL*)malloc(sizeof(FlexQL));
    (*db)->sockfd = sock;
    return FLEXQL_OK;
}

// Safely terminates the socket connection and releases the handle payload
int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR;
    close(db->sockfd);
    free(db);
    return FLEXQL_OK;
}


// 2. DESERIALIZATION ENGINE
// Parses the custom wire protocol (Pipe-delimited rows, comma-delimited columns) and maps it to a standard C-style callback mechanism (similar to sqlite3_exec)
void process_select_result(const std::string& response, int (*callback)(void*, int, char**, char**), void* arg) {
    if (!callback) return;

    std::stringstream ss(response);
    std::string item;
    std::vector<std::string> rows;
    
    // Split payload into discrete tuples
    while (std::getline(ss, item, '|')) {
        if (!item.empty()) rows.push_back(item);
    }
    if (rows.empty()) return;

    // Extract schema header (always the first tuple)
    std::vector<std::string> headers;
    std::stringstream header_ss(rows[0]);
    std::string h;
    while (std::getline(header_ss, h, ',')) headers.push_back(h);
    
    int colCount = headers.size();
    char** colNames = (char**)malloc(colCount * sizeof(char*));
    for (int i = 0; i < colCount; ++i) colNames[i] = strdup(headers[i].c_str());

    // Iteratively extract data tuples and invoke the client callback
    for (size_t i = 1; i < rows.size(); ++i) {
        std::vector<std::string> vals;
        std::stringstream val_ss(rows[i]);
        std::string v;
        while (std::getline(val_ss, v, ',')) vals.push_back(v);

        char** colVals = (char**)malloc(colCount * sizeof(char*));
        for (int j = 0; j < colCount; ++j) {
            colVals[j] = j < vals.size() ? strdup(vals[j].c_str()) : strdup("NULL");
        }

        // Halt processing if the client callback returns a non-zero interrupt signal
        if (callback(arg, colCount, colVals, colNames) != 0) break;

        for (int j = 0; j < colCount; ++j) free(colVals[j]);
        free(colVals);
    }

    for (int i = 0; i < colCount; ++i) free(colNames[i]);
    free(colNames);
}


// 3. QUERY EXECUTION
// Synchronous dispatch of SQL payloads over the network boundary
int flexql_exec(FlexQL *db, const char *sql, int (*callback)(void*, int, char**, char**), void *arg, char **errmsg) {
    if (!db) return FLEXQL_ERROR;

    send(db->sockfd, sql, strlen(sql), 0);

    // Block awaiting server resolution
    char buffer[4096] = {0};
    int valread = read(db->sockfd, buffer, 4096);
    if (valread <= 0) {
        *errmsg = strdup("Connection lost.");
        return FLEXQL_ERROR;
    }

    std::string response(buffer);
    
    // Check for internal engine fault flags
    if (response.find("ERROR") == 0) {
        *errmsg = strdup(response.c_str());
        return FLEXQL_ERROR;
    }

    // Identify serialized SELECT payloads and route to the parsing engine
    if (response.find("|") != std::string::npos) {
        process_select_result(response, callback, arg);
    }

    return FLEXQL_OK;
}

// Utility to free dynamically allocated error messages returned by flexql_exec
void flexql_free(void *ptr) {
    if (ptr) free(ptr);
}