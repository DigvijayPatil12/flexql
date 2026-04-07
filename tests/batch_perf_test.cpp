#include <iostream>
#include <vector>
#include <fstream>
#include <iomanip>
#include <chrono>    
#include <sstream>   
#include "flexql.h"

using namespace std;
using namespace std::chrono;

void run_batch_experiment(FlexQL* db, int batch_size, long long total_rows) {
    string tName = "BATCH_TEST_" + to_string(batch_size);
    char *err = nullptr;
    
    flexql_exec(db, ("CREATE TABLE " + tName + "(ID INT, VAL DECIMAL);").c_str(), nullptr, nullptr, &err);

    auto start = high_resolution_clock::now();
    long long inserted = 0;
    
    while (inserted < total_rows) {
        stringstream ss;
        ss << "INSERT INTO " << tName << " VALUES ";
        
        int current_batch_count = 0;
        while (current_batch_count < batch_size && inserted < total_rows) {
            ss << "(" << ++inserted << ", 100.5)";
            current_batch_count++;
            if (current_batch_count < batch_size && inserted < total_rows) ss << ",";
        }
        ss << ";";
        
        flexql_exec(db, ss.str().c_str(), nullptr, nullptr, nullptr);
    }
    
    auto end = high_resolution_clock::now();
    long long ms = duration_cast<milliseconds>(end - start).count();
    long long throughput = (ms > 0) ? (total_rows * 1000 / ms) : total_rows;
    
    ofstream csv("batch_experiment.csv", ios::app);
    csv << batch_size << "," << ms << "," << throughput << "\n";
    
    cout << "Batch " << setw(6) << batch_size 
         << " | Time: " << setw(8) << ms << " ms"
         << " | Throughput: " << setw(10) << throughput << " r/s" << endl;
    
    flexql_exec(db, ("DROP TABLE " + tName + ";").c_str(), nullptr, nullptr, nullptr);
}

int main() {
    FlexQL *db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cout << "Error: Could not connect to FlexQL server." << endl;
        return 1;
    }
    
    ofstream csv("batch_experiment.csv");
    csv << "BatchSize,Time_MS,Throughput\n";
    csv.close();
    
    cout << "Starting Batch Size Experiment (1M rows per test)..." << endl;
    cout << "----------------------------------------------------" << endl;
    
    vector<int> batch_sizes = {1, 10, 100, 500, 1000, 2500, 5000, 10000};
    for (int bs : batch_sizes) run_batch_experiment(db, bs, 1000000); 
    
    flexql_close(db);
    cout << "----------------------------------------------------" << endl;
    cout << "Experiment complete. Data saved to batch_experiment.csv" << endl;
    
    return 0;
}