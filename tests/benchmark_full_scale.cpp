#include <iostream>
#include <chrono>
#include <string>
#include <sstream>
#include <vector>
#include <iomanip>
#include <fstream>
#include <cmath>
#include "flexql.h"

using namespace std;
using namespace std::chrono;

static const int INSERT_BATCH_SIZE = 5000; 

struct BenchResult { 
    long long target_rows; 
    long long elapsed_ms; 
    long long throughput; 
};

static bool run_exec(FlexQL *db, const string &sql) {
    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    if (rc != FLEXQL_OK) {
        if (errMsg) flexql_free(errMsg);
        return false;
    }
    return true;
}

static BenchResult run_test_iteration(FlexQL *db, long long target_rows) {
    string tName = "BENCH_" + to_string(target_rows);
    
    run_exec(db, "DROP TABLE " + tName + ";");
    run_exec(db, "CREATE TABLE " + tName + "(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);");

    auto bench_start = high_resolution_clock::now();
    long long inserted = 0;

    while (inserted < target_rows) {
        stringstream ss;
        ss << "INSERT INTO " << tName << " VALUES ";

        int in_batch = 0;
        while (in_batch < INSERT_BATCH_SIZE && inserted < target_rows) {
            long long id = inserted + 1;
            ss << "(" << id << ", 'user" << id << "', 'user" << id << "@mail.com', " << (1000.0 + (id % 10000)) << ", 1893456000)";
            inserted++;
            in_batch++;
            if (in_batch < INSERT_BATCH_SIZE && inserted < target_rows) ss << ",";
        }
        ss << ";";

        char *errMsg = nullptr;
        flexql_exec(db, ss.str().c_str(), nullptr, nullptr, &errMsg);
        if (errMsg) flexql_free(errMsg);
    }

    auto bench_end = high_resolution_clock::now();
    
    run_exec(db, "DROP TABLE " + tName + ";");

    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    long long throughput = (elapsed > 0) ? (target_rows * 1000LL / elapsed) : target_rows;

    return {target_rows, elapsed, throughput};
}

int main(int argc, char **argv) {
    FlexQL *db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cout << "Cannot connect to server.\n";
        return 1;
    }

    vector<long long> targets;
    for (int i = 2; i <= 7; ++i) {
        targets.push_back(pow(10, i));
        if (i < 7) targets.push_back(pow(10, i) * 5);
    }
    targets.push_back(20000000); 

    vector<BenchResult> results;
    ofstream csv("benchmark_results.csv");
    csv << "Rows,Time_MS,Throughput_Rows_Sec\n";

    cout << "\n" << string(50, '=') << "\n";
    cout << "      FLEXQL DEEP-DIVE SCALABILITY TEST\n";
    cout << string(50, '=') << "\n\n";

    for (long long target : targets) {
        cout << "Testing " << setw(10) << target << " rows... " << flush;
        BenchResult res = run_test_iteration(db, target);
        results.push_back(res);
        
        csv << res.target_rows << "," << res.elapsed_ms << "," << res.throughput << "\n";
        cout << "DONE (" << res.elapsed_ms << " ms | " << res.throughput << " r/s)\n";
    }

    csv.close();
    flexql_close(db);

    cout << "\n" << string(50, '=') << "\n";
    cout << "REPORT GENERATED: benchmark_results.csv\n";
    cout << string(50, '=') << "\n";

    return 0;
}