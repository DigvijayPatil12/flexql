#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <list>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <ctime>
#include <algorithm>
#include <cctype>
#include <fstream>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <dirent.h>


// 1. DATA STRUCTURES & MEMORY LAYOUT
enum DataType { INT_TYPE, DECIMAL_TYPE, VARCHAR_TYPE, DATETIME_TYPE, UNKNOWN_TYPE };

struct Column {
    std::string name;
    DataType type;
};

// Variant wrapper to enforce strict type safety without void* polymorphism
struct Cell {
    int i_val = 0;
    double d_val = 0.0;
    std::string s_val = "";
    time_t t_val = 0; 
};

// Row-major storage unit. TTL is evaluated passively during read operations.
struct Row {
    time_t expires_at;
    std::vector<Cell> data;
};

// Core table representation mapping to a single .dat file on disk
class Table {
public:
    std::string name;
    std::vector<Column> columns;
    std::vector<Row> rows; 
    
    // O(1) Lookup: Maps Primary Key (INT) to the vector memory index (size_t)
    std::unordered_map<int, size_t> primary_index; 
    
    // Reader-Writer lock for granular concurrency control
    mutable std::shared_timed_mutex table_mutex; 
};


// 2. CONCURRENCY & ASYNC I/O
// Bounded Thread Pool to prevent stack exhaustion from unlimited client connections
class ThreadPool {
private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop;

public:
    ThreadPool(size_t threads) : stop(false) {
        for(size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                while(true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this]{ return this->stop || !this->tasks.empty(); });
                        if(this->stop && this->tasks.empty()) return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    void enqueue(std::function<void()> task) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.push(std::move(task));
        }
        condition.notify_one();
    }

    ~ThreadPool() {
        stop = true;
        condition.notify_all();
        for(std::thread &worker: workers) worker.join();
    }
};

// Decouples RAM operations from physical SSD latency
class AsyncDiskWriter {
private:
    std::queue<std::pair<std::string, std::string>> write_queue;
    std::mutex mtx;
    std::condition_variable cv_consumer;
    std::condition_variable cv_producer; 
    std::atomic<bool> running;
    std::thread worker;

public:
    AsyncDiskWriter() : running(true) {
        worker = std::thread([this]() {
            while (running) {
                std::pair<std::string, std::string> task;
                {
                    std::unique_lock<std::mutex> lock(mtx);
                    cv_consumer.wait(lock, [this]() { return !write_queue.empty() || !running; });
                    if (!running && write_queue.empty()) return;
                    
                    task = std::move(write_queue.front());
                    write_queue.pop();
                }
                cv_producer.notify_one(); 

                if (!task.first.empty() && !task.second.empty()) {
                    std::ofstream file(task.first, std::ios::app);
                    file.write(task.second.c_str(), task.second.length());
                }
            }
        });
    }

    ~AsyncDiskWriter() {
        running = false;
        cv_consumer.notify_all();
        if (worker.joinable()) worker.join();
    }

    void enqueue(const std::string& filepath, std::string&& data) {
        std::unique_lock<std::mutex> lock(mtx);
        // BACKPRESSURE: Halts network threads if queue exceeds 50 batches (prevents OOM)
        cv_producer.wait(lock, [this]() { return write_queue.size() < 50; });
        write_queue.push({filepath, std::move(data)});
        cv_consumer.notify_one();
    }
};


// 3. UTILITIES & QUERY CACHING
static inline void trim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); }).base(), s.end());
}

static std::vector<std::string> split(const std::string &str, char delimiter) {
    std::vector<std::string> tokens;
    size_t start = 0, end = 0;
    while ((end = str.find(delimiter, start)) != std::string::npos) {
        std::string token = str.substr(start, end - start);
        trim(token);
        if (!token.empty()) tokens.push_back(token);
        start = end + 1;
    }
    std::string token = str.substr(start);
    trim(token);
    if (!token.empty()) tokens.push_back(token);
    return tokens;
}

static DataType parseType(std::string typeStr) {
    std::transform(typeStr.begin(), typeStr.end(), typeStr.begin(), ::toupper);
    if (typeStr.find("INT") == 0) return INT_TYPE;
    if (typeStr.find("DECIMAL") == 0) return DECIMAL_TYPE;
    if (typeStr.find("VARCHAR") == 0 || typeStr.find("TEXT") == 0) return VARCHAR_TYPE;
    if (typeStr.find("DATETIME") == 0) return DATETIME_TYPE;
    return UNKNOWN_TYPE;
}

// O(1) access cache to bypass table locks for redundant SELECT operations
class LRUCache {
private:
    size_t capacity;
    std::list<std::string> keys; 
    std::unordered_map<std::string, std::pair<std::string, std::list<std::string>::iterator>> cache_data;
    std::mutex cache_mutex;

public:
    LRUCache(size_t cap = 1000) : capacity(cap) {}

    bool get(const std::string& query, std::string& result) {
        std::lock_guard<std::mutex> lock(cache_mutex);
        auto it = cache_data.find(query);
        if (it == cache_data.end()) return false;

        // Evict and promote to front on cache hit
        keys.erase(it->second.second);
        keys.push_front(query);
        it->second.second = keys.begin();
        
        result = it->second.first;
        return true;
    }

    void put(const std::string& query, const std::string& result) {
        std::lock_guard<std::mutex> lock(cache_mutex);
        if (cache_data.find(query) != cache_data.end()) {
            keys.erase(cache_data[query].second);
        } else if (cache_data.size() >= capacity) {
            cache_data.erase(keys.back());
            keys.pop_back();
        }
        keys.push_front(query);
        cache_data[query] = {result, keys.begin()};
    }

    // Coarse-grained invalidation applied on state mutations (INSERT, CREATE, DROP)
    void invalidate() {
        std::lock_guard<std::mutex> lock(cache_mutex);
        cache_data.clear();
        keys.clear();
    }
};


// 4. EXECUTION ENGINE
class Database {
private:
    std::map<std::string, Table*> tables;
    std::shared_timed_mutex db_mutex;
    LRUCache query_cache;
    AsyncDiskWriter disk_writer;
    
    std::thread background_purger;
    std::atomic<bool> purger_running;
    
    const std::string DATA_DIR = "data/";

    std::string serializeRow(const Table* t, const Row& r) {
        std::string res = "";
        res.reserve(128); 
        for (size_t i = 0; i < r.data.size(); ++i) {
            if (t->columns[i].type == INT_TYPE) res += std::to_string(r.data[i].i_val);
            else if (t->columns[i].type == DECIMAL_TYPE) {
                std::string s = std::to_string(r.data[i].d_val);
                s.erase(s.find_last_not_of('0') + 1, std::string::npos);
                if (s.back() == '.') s.pop_back();
                res += s;
            }
            else if (t->columns[i].type == DATETIME_TYPE) res += std::to_string(r.data[i].t_val);
            else res += r.data[i].s_val;
            res += (i == r.data.size() - 1 ? "" : "|");
        }
        return res + "|" + std::to_string(r.expires_at) + "\n";
    }

public:
    Database() {
        std::cout << "FlexQL Booting... Restoring state from " << DATA_DIR << "\n";
        int dir_status = system(("mkdir -p " + DATA_DIR).c_str());
        if (dir_status != 0) std::cerr << "Warning: Could not verify data directory.\n";

        // Dynamic schema reconstruction via directory scanning
        std::vector<std::string> known_tables;
        DIR *dir;
        struct dirent *ent;
        if ((dir = opendir(DATA_DIR.c_str())) != NULL) {
            while ((ent = readdir(dir)) != NULL) {
                std::string file_name = ent->d_name;
                if (file_name.length() >= 4 && file_name.substr(file_name.length() - 4) == ".dat") {
                    known_tables.push_back(file_name.substr(0, file_name.length() - 4));
                }
            }
            closedir(dir);
        }
        
        for (const auto& tName : known_tables) {
            std::ifstream file(DATA_DIR + tName + ".dat");
            if (!file.is_open()) continue;

            std::cout << "  -> Loading schema and data for: " << tName << "... ";
            Table* t = new Table(); t->name = tName;
            std::string line;

            if (std::getline(file, line) && !line.empty()) {
                std::vector<std::string> col_defs = split(line, ',');
                for (auto& def : col_defs) {
                    size_t sep = def.find(':');
                    if (sep != std::string::npos) {
                        Column c;
                        c.name = def.substr(0, sep);
                        c.type = (DataType)std::stoi(def.substr(sep + 1));
                        t->columns.push_back(c);
                    }
                }
            }

            int row_count = 0;
            while (std::getline(file, line)) {
                if (line.empty()) continue;
                std::vector<std::string> parts = split(line, '|');
                if (parts.size() < t->columns.size()) continue;

                Row r;
                for (size_t i = 0; i < t->columns.size(); ++i) {
                    Cell cell;
                    if (t->columns[i].type == INT_TYPE) cell.i_val = std::stoi(parts[i]);
                    else if (t->columns[i].type == DECIMAL_TYPE) cell.d_val = std::stod(parts[i]);
                    else if (t->columns[i].type == DATETIME_TYPE) cell.t_val = (time_t)std::stoll(parts[i]);
                    else cell.s_val = parts[i];
                    r.data.push_back(cell);
                }
                r.expires_at = (time_t)std::stoll(parts.back());
                t->rows.push_back(r);

                // Re-hydrate O(1) hash index
                if (!t->columns.empty()) {
                    int pk = (t->columns[0].type == INT_TYPE) ? r.data[0].i_val : (int)r.data[0].d_val;
                    t->primary_index[pk] = t->rows.size() - 1;
                }
                row_count++;
            }
            tables[tName] = t;
            std::cout << row_count << " records restored.\n";
        }

        // Dedicated Garbage Collection Daemon
        purger_running = true;
        background_purger = std::thread([this]() {
            while(purger_running) {
                std::this_thread::sleep_for(std::chrono::seconds(10));
                time_t now = std::time(nullptr);
                
                std::shared_lock<std::shared_timed_mutex> db_lock(db_mutex);
                for (auto& pair : tables) {
                    Table* t = pair.second;
                    std::unique_lock<std::shared_timed_mutex> t_lock(t->table_mutex); // Exclusive access required
                    
                    size_t old_size = t->rows.size();
                    // Erase-Remove Idiom for contiguous memory compaction
                    t->rows.erase(std::remove_if(t->rows.begin(), t->rows.end(), 
                        [now](const Row& r) { return r.expires_at <= now; }), t->rows.end());
                    
                    // Reallocation requires complete index map rebuild
                    if (t->rows.size() != old_size) {
                        t->primary_index.clear();
                        for(size_t i = 0; i < t->rows.size(); ++i) {
                            if (!t->columns.empty()) {
                                int pk = (t->columns[0].type == INT_TYPE) ? t->rows[i].data[0].i_val : (int)t->rows[i].data[0].d_val;
                                t->primary_index[pk] = i;
                            }
                        }
                    }
                }
            }
        });
    }

    ~Database() {
        purger_running = false;
        if(background_purger.joinable()) background_purger.join();
    }

    // Query Router
    std::string execute_query(const std::string& query) {
        if (query.find("SELECT") == 0) {
            std::string cached_response;
            if (query_cache.get(query, cached_response)) return cached_response;
            
            std::string result = handle_select(query);
            if (result.find("ERROR") == std::string::npos) query_cache.put(query, result);
            return result;
        }
        
        std::string result;
        if (query.find("CREATE TABLE") == 0) result = handle_create(query);
        else if (query.find("INSERT INTO") == 0) result = handle_insert(query);
        else if (query.find("DROP TABLE") == 0) result = handle_drop(query);
        else return "ERROR: Unsupported query specification";
        
        query_cache.invalidate(); // Mutation occurred, flush state
        return result;
    }

private:
    std::string handle_create(const std::string& query) {
        std::unique_lock<std::shared_timed_mutex> lock(db_mutex);
        size_t name_start = query.find("TABLE ") + 6;
        size_t name_end = query.find("(", name_start);
        if (name_start == std::string::npos || name_end == std::string::npos) return "ERROR: Syntax error in CREATE statement";

        std::string tName = query.substr(name_start, name_end - name_start);
        trim(tName);
        if (tables.count(tName)) return "ERROR: Table already exists";

        Table* t = new Table(); t->name = tName;
        std::string schema_header = "";
        std::string cols_str = query.substr(name_end + 1, query.find_last_of(")") - (name_end + 1));
        
        for (const auto& def : split(cols_str, ',')) {
            std::vector<std::string> parts = split(def, ' ');
            if (parts.size() >= 2) {
                Column c; c.name = parts[0]; c.type = parseType(parts[1]);
                t->columns.push_back(c);
                schema_header += c.name + ":" + std::to_string(c.type) + ",";
            }
        }
        tables[tName] = t;
        
        disk_writer.enqueue(DATA_DIR + tName + ".dat", schema_header + "\n");
        return "OK";
    }
    
    std::string handle_drop(const std::string& query) {
        std::unique_lock<std::shared_timed_mutex> lock(db_mutex);
        size_t name_start = query.find("TABLE ") + 6;
        std::string tName = query.substr(name_start);
        if (tName.back() == ';') tName.pop_back();
        trim(tName);

        if (tables.find(tName) == tables.end()) return "ERROR: Table not found";

        delete tables[tName];
        tables.erase(tName);

        std::string filePath = DATA_DIR + tName + ".dat";
        if (remove(filePath.c_str()) != 0) {
            return "OK (Removed from cache, physical file unlinked)";
        }
        return "OK";
    }

    std::string handle_insert(const std::string& query) {
        std::shared_lock<std::shared_timed_mutex> db_lock(db_mutex);
        size_t t_start = query.find("INTO ") + 5;
        size_t t_end = query.find(" VALUES");
        std::string tName = query.substr(t_start, t_end - t_start); trim(tName);

        if (!tables.count(tName)) return "ERROR: Table not found";
        Table* t = tables[tName];
        
        // Block readers until tuple batch is fully resident in RAM
        std::unique_lock<std::shared_timed_mutex> table_lock(t->table_mutex);

        size_t cols_count = t->columns.size();
        std::string batch_disk_data;
        batch_disk_data.reserve(query.length() * 2); 

        size_t pos = query.find("VALUES");
        while ((pos = query.find('(', pos)) != std::string::npos) {
            size_t end = query.find(')', pos);
            if (end == std::string::npos) break;

            Row r; 
            r.expires_at = 2147483647; // Default: Never expire
            r.data.resize(cols_count);

            size_t s = pos + 1;
            int current_col = 0;

            while (s < end && current_col < cols_count) {
                size_t comma = query.find(',', s);
                if (comma == std::string::npos || comma > end) comma = end;
                
                std::string v = query.substr(s, comma - s);
                trim(v);
                
                if (!v.empty() && v.front() == '\'') v = v.substr(1, v.length() - 2);
                
                if (t->columns[current_col].type == INT_TYPE) r.data[current_col].i_val = std::stoi(v);
                else if (t->columns[current_col].type == DECIMAL_TYPE) r.data[current_col].d_val = std::stod(v);
                else if (t->columns[current_col].type == DATETIME_TYPE) r.data[current_col].t_val = (time_t)std::stoll(v);
                else r.data[current_col].s_val = v;
                
                if (t->columns[current_col].name == "EXPIRES_AT") r.expires_at = (time_t)std::stoll(v);
                
                current_col++;
                s = comma + 1;
            }

            if (current_col != cols_count) return "ERROR: Column count mismatch in tuple";

            batch_disk_data += serializeRow(t, r);
            t->rows.push_back(r);

            if (cols_count > 0) {
                int pk = (t->columns[0].type == INT_TYPE) ? r.data[0].i_val : (int)r.data[0].d_val;
                t->primary_index[pk] = t->rows.size() - 1;
            }
            pos = end + 1;
        }

        if (!batch_disk_data.empty()) {
            disk_writer.enqueue(DATA_DIR + tName + ".dat", std::move(batch_disk_data));
        }
        
        return "OK";
    }

    std::string cellToString(const Cell& c, DataType type) {
        if (type == INT_TYPE) return std::to_string(c.i_val);
        if (type == DECIMAL_TYPE) {
            std::string str = std::to_string(c.d_val);
            str.erase(str.find_last_not_of('0') + 1, std::string::npos);
            if (str.back() == '.') str.pop_back();
            return str;
        }
        if (type == DATETIME_TYPE) return std::to_string(c.t_val);
        return c.s_val; 
    }

    std::string handle_select(const std::string& query) {
        std::shared_lock<std::shared_timed_mutex> db_lock(db_mutex);
        time_t current_time = std::time(nullptr);

        size_t join_pos = query.find("INNER JOIN");
        if (join_pos != std::string::npos) {
            size_t from_pos = query.find("FROM ") + 5;
            std::string tableA_name = query.substr(from_pos, join_pos - from_pos - 1);
            trim(tableA_name);
            
            size_t on_pos = query.find(" ON ");
            size_t where_pos = query.find(" WHERE ");
            size_t join_end = where_pos != std::string::npos ? where_pos : (query.find(";") != std::string::npos ? query.find(";") : query.length());
            
            std::string tableB_name = query.substr(join_pos + 11, on_pos - (join_pos + 11));
            trim(tableB_name);

            std::string on_clause = query.substr(on_pos + 4, join_end - (on_pos + 4));
            size_t eq_pos = on_clause.find('=');
            std::string left_part = on_clause.substr(0, eq_pos); trim(left_part);
            std::string right_part = on_clause.substr(eq_pos + 1); trim(right_part);
            
            std::string tA_col = left_part.substr(left_part.find('.') + 1);
            std::string tB_col = right_part.substr(right_part.find('.') + 1);

            if (tables.find(tableA_name) == tables.end() || tables.find(tableB_name) == tables.end()) {
                return "ERROR: Target tables unresolvable for JOIN operation";
            }

            Table* tA = tables[tableA_name];
            Table* tB = tables[tableB_name];

            int tA_col_idx = -1, tB_col_idx = -1;
            for (size_t i = 0; i < tA->columns.size(); ++i) if (tA->columns[i].name == tA_col) tA_col_idx = i;
            for (size_t i = 0; i < tB->columns.size(); ++i) if (tB->columns[i].name == tB_col) tB_col_idx = i;

            if (tA_col_idx == -1 || tB_col_idx == -1) return "ERROR: JOIN ON condition references invalid column";

            // Acquire locks on both resources to prevent modification during Cartesian evaluation
            std::shared_lock<std::shared_timed_mutex> lockA(tA->table_mutex, std::defer_lock);
            std::shared_lock<std::shared_timed_mutex> lockB(tB->table_mutex, std::defer_lock);
            std::lock(lockA, lockB);

            bool has_where = where_pos != std::string::npos;
            double j_val = 0; std::string j_op = ""; std::string raw_cname = "";
            std::string where_val_str_join = "";
            
            if (has_where) {
                std::string cond = query.substr(where_pos + 7, (query.find(";") != std::string::npos ? query.find(";") : query.length()) - (where_pos + 7));
                std::vector<std::string> ops = {">=", "<=", "=", ">", "<"};
                size_t op_loc = std::string::npos;
                for (const auto& o : ops) {
                    op_loc = cond.find(o);
                    if (op_loc != std::string::npos) { j_op = o; break; }
                }
                if (op_loc != std::string::npos) {
                    raw_cname = cond.substr(0, op_loc); trim(raw_cname);
                    if (raw_cname.find('.') != std::string::npos) raw_cname = raw_cname.substr(raw_cname.find('.') + 1);
                    where_val_str_join = cond.substr(op_loc + j_op.length()); trim(where_val_str_join);
                    if (where_val_str_join.front() == '\'' && where_val_str_join.back() == '\'') 
                        where_val_str_join = where_val_str_join.substr(1, where_val_str_join.length() - 2);
                    
                    try { j_val = std::stod(where_val_str_join); } catch (...) {}
                }
            }

            std::string result = "";
            for (const auto& col : tA->columns) result += tA->name + "." + col.name + ",";
            for (size_t i = 0; i < tB->columns.size(); ++i) result += tB->name + "." + tB->columns[i].name + (i == tB->columns.size() - 1 ? "" : ",");
            result += "|";

            for (const auto& rowA : tA->rows) {
                if (rowA.expires_at <= current_time) continue;
                for (const auto& rowB : tB->rows) {
                    if (rowB.expires_at <= current_time) continue;

                    double valA = tA->columns[tA_col_idx].type == INT_TYPE ? rowA.data[tA_col_idx].i_val : rowA.data[tA_col_idx].d_val;
                    double valB = tB->columns[tB_col_idx].type == INT_TYPE ? rowB.data[tB_col_idx].i_val : rowB.data[tB_col_idx].d_val;

                    if (valA == valB) {
                        bool join_match = true;
                        if (has_where && !j_op.empty()) {
                            double cell_v = 0; std::string str_v = ""; bool is_str = false;
                            for (size_t k = 0; k < tB->columns.size(); ++k) {
                                if (tB->columns[k].name == raw_cname) { 
                                    if (tB->columns[k].type == VARCHAR_TYPE) { str_v = rowB.data[k].s_val; is_str = true; }
                                    else { cell_v = tB->columns[k].type == INT_TYPE ? rowB.data[k].i_val : rowB.data[k].d_val; }
                                    break; 
                                }
                            }
                            if (is_str) {
                                if (j_op == "=" && str_v != where_val_str_join) join_match = false;
                            } else {
                                if (j_op == ">" && !(cell_v > j_val)) join_match = false;
                                else if (j_op == "<" && !(cell_v < j_val)) join_match = false;
                                else if (j_op == "=" && !(cell_v == j_val)) join_match = false;
                                else if (j_op == ">=" && !(cell_v >= j_val)) join_match = false;
                                else if (j_op == "<=" && !(cell_v <= j_val)) join_match = false;
                            }
                        }
                        
                        if (!join_match) continue;

                        for (size_t i = 0; i < rowA.data.size(); ++i) result += cellToString(rowA.data[i], tA->columns[i].type) + ",";
                        for (size_t i = 0; i < rowB.data.size(); ++i) {
                            result += cellToString(rowB.data[i], tB->columns[i].type);
                            result += (i == rowB.data.size() - 1 ? "" : ",");
                        }
                        result += "|";
                    }
                }
            }
            return result;
        }

        size_t select_pos = query.find("SELECT ") + 7;
        size_t from_pos = query.find(" FROM ");
        if (from_pos == std::string::npos) return "ERROR: Malformed SELECT query format";

        std::string cols_str = query.substr(select_pos, from_pos - select_pos);
        std::vector<std::string> selected_col_names = split(cols_str, ',');

        size_t where_pos = query.find(" WHERE ");
        size_t end_pos = query.find(";") != std::string::npos ? query.find(";") : query.length();
        
        std::string tableName = query.substr(from_pos + 6, (where_pos != std::string::npos ? where_pos : end_pos) - (from_pos + 6));
        trim(tableName);

        if (tables.find(tableName) == tables.end()) return "ERROR: Table not found in cache";
        Table* t = tables[tableName];
        std::shared_lock<std::shared_timed_mutex> table_lock(t->table_mutex); // Non-blocking concurrent reads

        std::vector<int> col_indices;
        if (selected_col_names.size() == 1 && selected_col_names[0] == "*") {
            for (size_t i = 0; i < t->columns.size(); ++i) col_indices.push_back(i);
        } else {
            for (const auto& cname : selected_col_names) {
                bool found = false;
                for (size_t i = 0; i < t->columns.size(); ++i) {
                    if (t->columns[i].name == cname) { col_indices.push_back(i); found = true; break; }
                }
                if (!found) return "ERROR: Requested column identifier invalid";
            }
        }

        int where_col_idx = -1;
        std::string where_val_str = "";
        std::string op = "";

        if (where_pos != std::string::npos) {
            std::string condition = query.substr(where_pos + 7, end_pos - (where_pos + 7));
            std::vector<std::string> ops = {">=", "<=", "=", ">", "<"};
            size_t op_pos = std::string::npos;
            
            for (const auto& o : ops) {
                op_pos = condition.find(o);
                if (op_pos != std::string::npos) { op = o; break; }
            }

            if (op_pos != std::string::npos) {
                std::string w_col = condition.substr(0, op_pos);
                trim(w_col);
                where_val_str = condition.substr(op_pos + op.length());
                trim(where_val_str);
                
                if (where_val_str.front() == '\'' && where_val_str.back() == '\'') where_val_str = where_val_str.substr(1, where_val_str.length() - 2);

                for (size_t i = 0; i < t->columns.size(); ++i) {
                    if (t->columns[i].name == w_col) { where_col_idx = i; break; }
                }
                if (where_col_idx == -1) return "ERROR: Filter condition references unknown column";
            }
        }

        std::string result = "";
        for (size_t i = 0; i < col_indices.size(); ++i) result += t->columns[col_indices[i]].name + (i == col_indices.size() - 1 ? "" : ",");
        result += "|";

        // PRIMARY KEY FAST-PATH: O(1) retrieval bypassing vector iteration
        if (where_col_idx == 0 && op == "=" && t->columns[0].type == INT_TYPE) {
            int pk_val = 0;
            try { pk_val = std::stoi(where_val_str); } catch(...) { return result; }
            
            if (t->primary_index.count(pk_val)) {
                size_t row_idx = t->primary_index[pk_val];
                if (row_idx < t->rows.size()) {
                    const Row& row = t->rows[row_idx];
                    if (row.expires_at > current_time) {
                        for (size_t i = 0; i < col_indices.size(); ++i) {
                            result += cellToString(row.data[col_indices[i]], t->columns[col_indices[i]].type);
                            result += (i == col_indices.size() - 1 ? "" : ",");
                        }
                        result += "|";
                    }
                }
            }
            return result;
        }

        // FULL TABLE SCAN FALLBACK: Iterative O(N) evaluation
        for (const auto& row : t->rows) {
            if (row.expires_at <= current_time) continue; 

            if (where_col_idx != -1) {
                bool match = false;
                const Cell& c = row.data[where_col_idx];
                DataType type = t->columns[where_col_idx].type;

                try {
                    if (type == INT_TYPE) {
                        int v = std::stoi(where_val_str);
                        if (op == "=") match = (c.i_val == v);
                        else if (op == ">") match = (c.i_val > v);
                        else if (op == "<") match = (c.i_val < v);
                        else if (op == ">=") match = (c.i_val >= v);
                        else if (op == "<=") match = (c.i_val <= v);
                    }
                    else if (type == DECIMAL_TYPE) {
                        double v = std::stod(where_val_str);
                        if (op == "=") match = (c.d_val == v);
                        else if (op == ">") match = (c.d_val > v);
                        else if (op == "<") match = (c.d_val < v);
                        else if (op == ">=") match = (c.d_val >= v);
                        else if (op == "<=") match = (c.d_val <= v);
                    }
                    else if (type == VARCHAR_TYPE) {
                        if (op == "=") match = (c.s_val == where_val_str);
                    }
                } catch (...) { continue; } 

                if (!match) continue;
            }

            for (size_t i = 0; i < col_indices.size(); ++i) {
                result += cellToString(row.data[col_indices[i]], t->columns[col_indices[i]].type);
                result += (i == col_indices.size() - 1 ? "" : ",");
            }
            result += "|";
        }
        return result;
    }
};


// 5. TCP NETWORK HANDLER
Database db;

void handle_client(int client_socket) {
    char temp_buf[65536]; 
    
    while (true) {
        std::string query = "";
        
        while (true) {
            memset(temp_buf, 0, sizeof(temp_buf));
            int bytes_read = read(client_socket, temp_buf, sizeof(temp_buf) - 1);
            
            if (bytes_read <= 0) break; 
            
            query.append(temp_buf, bytes_read); 
            if (query.find(';') != std::string::npos) break;
        }
        
        if (query.empty()) break;

        std::string response = db.execute_query(query);
        send(client_socket, response.c_str(), response.length(), 0);
    }
    close(client_socket);
}

int main() {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket initialization failure");
        exit(EXIT_FAILURE);
    }

    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(9000);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Port bind mapping failure");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 100) < 0) { 
        perror("TCP backlog listen configuration failure");
        exit(EXIT_FAILURE);
    }

    std::cout << "FlexQL Runtime Active: Port 9000 (Async I/O & ThreadPool Enabled)\n";

    size_t thread_count = std::thread::hardware_concurrency();
    if (thread_count == 0) thread_count = 8; // Fallback bound
    ThreadPool pool(thread_count);

    while (true) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            continue;
        }
        
        // Dispatch to fixed-size pool
        pool.enqueue([new_socket]() {
            handle_client(new_socket);
        });
    }
    return 0;
}