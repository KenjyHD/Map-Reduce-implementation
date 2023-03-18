#include <iostream>
#include <cstdlib>
#include <pthread.h>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <set>
#include <math.h>

using namespace std;

//struct of parameters which will be sent to thread f function
struct Parameters {
    int is_mapper;
    long id;
    pthread_t thread;
    ifstream *infile;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    vector<vector<vector<int>>> *mapper_list;
    vector<set<int>> *reducer_list;
};

//function that search whether a number is a perfect power, insert the number 
//in mapper list at the section of the found power
void search_perfect_powers(int id, int nr, int max_power, 
    vector<vector<vector<int>>> *list) {

    float error = 0.1;
    //search whether the number is a perfect power with i being the power
    for (int i = 2; i <= max_power; i++) {
        if (nr == 1) {
            list->at(id).at(i - 2).push_back(nr);
        }
        float start = 1;
        float end = nr;
        float mid = 0;
        unsigned long multiplication_val = 1;
        //use binary search for finding the base that, raised to i power, 
        //results the given number 
        while (abs(start - end) > error) {
            mid = (start + end) / 2;
            multiplication_val = (unsigned long)pow(round(mid), i);
            
            if(multiplication_val == 0) {
                multiplication_val = UINT32_MAX;
            }
            //the number is a perfect power, i is the power
            if (multiplication_val == nr) {
                //add number to list
                list->at(id).at(i - 2).push_back(nr);
                break;
            }
                
            if (multiplication_val > nr) {
                end = mid;
            }

            if (multiplication_val < nr) {
                start = mid;
            }
        }
    }
}

//function that represents mapper's work(read the input from file, search 
//whether it's a perfect power and insert in in list)
void mapper_duty(int id, string file_name, vector<vector<vector<int>>> *list) {
    ifstream file;
    string line;
    file.open(file_name);
    getline(file, line);
    while (getline(file, line)) {
        int nr = stoi(line);
        //(number of lists + 1) for a mapper represents which will be the max_power
        int max_power = (list->at(id).size() + 1);
        search_perfect_powers(id, nr, max_power, list);
    }
    file.close();
}

//function that represents reducer's work(combine the partial lists for 
//an exponent in a single list retaining only unique elements, count them 
//and write the result in a file)
void reducer_duty(int index, vector<vector<vector<int>>> *mapper_list, 
    vector<set<int>> *reducer_list) {
    for (int i = 0; i < mapper_list->size(); i++) {
        for (int k = 0; k < mapper_list->at(i).at(index).size(); k++) {
            reducer_list->at(index).insert(mapper_list->at(i).at(index).at(k));
        }
    }
    string file_name = "out" + to_string(index + 2) + ".txt";
    ofstream file(file_name);
    file << reducer_list->at(index).size();
    file.close();
}

//thread function which separates the work of mappers and reducers
void *f(void *arg)
{
    struct Parameters thread = *(struct Parameters*) arg;
    long id = (*(struct Parameters*) arg).id;

    string file_name, line;
    ifstream file;
    int do_your_work = 1;

    if(thread.is_mapper) {
        //using this code structure the files are distributed 
        //dynamically whenever there is a free mapper
        while(do_your_work) {
            //set mutex to prevent the situation when 2 threads 
            //reads simultaneously from the test file
            pthread_mutex_lock(thread.mutex);
            if(!getline(*thread.infile, file_name)) {
                do_your_work = 0;
            }
            pthread_mutex_unlock(thread.mutex);
            
            if(do_your_work) {
                mapper_duty(id, file_name, thread.mapper_list);
            }
        }
        //wait for all the threads to reach the barrier
        pthread_barrier_wait(thread.barrier);
    } else {
        //after that start reducer's work
        pthread_barrier_wait(thread.barrier);
        int mapper_nr = thread.mapper_list->size();
        reducer_duty((id - mapper_nr), thread.mapper_list, thread.reducer_list);
    }
    pthread_exit(NULL);
}

int main(int argc, char** argv)
{
    int mappers_nr = stoi(argv[1]);
    int reducers_nr = stoi(argv[2]);
    ifstream infile;
    infile.open(argv[3]);
    int threads_nr = mappers_nr + reducers_nr;
    struct Parameters threads[threads_nr];
    
    int r;
    long id = 0;
    void *status;
    string line;
    pthread_mutex_t mutex;
    pthread_barrier_t barrier;
    vector<vector<vector<int>>> mapper_list;

    //set the sizes of mappers list
    mapper_list.resize(mappers_nr);
    for (int i = 0; i < mapper_list.size(); i++) {
        mapper_list.at(i).resize(reducers_nr);
    }
    vector<set<int>> reducer_list;

    //set the size of reducers list
    reducer_list.resize(reducers_nr);

    getline(infile, line);
    int file_nr = stoi(line);

    //initialize mutex and barrier
    pthread_mutex_init(&mutex, NULL);
    pthread_barrier_init(&barrier, NULL, threads_nr);

    for (id = 0; id < threads_nr; id++) {
        threads[id].mutex = &mutex;
        threads[id].barrier = &barrier;
        threads[id].mapper_list = &mapper_list;
        threads[id].reducer_list = &reducer_list;
        threads[id].infile = &infile;
        threads[id].id = id;
        if (id < mappers_nr) {
            threads[id].is_mapper = 1;
        }
        else {
            threads[id].is_mapper = 0;
        }
        
        r = pthread_create(&threads[id].thread, NULL, f, (void *) &threads[id]);
        
        if (r) {
            printf("Eroare la crearea thread-ului %ld\n", id);
            exit(-1);
        }
    }

    for (id = 0; id < threads_nr; id++) {
        r = pthread_join(threads[id].thread, &status);
 
        if (r) {
            printf("Eroare la asteptarea thread-ului %ld\n", id);
            exit(-1);
        }
    }

    pthread_mutex_destroy(&mutex);
    pthread_barrier_destroy(&barrier);
    infile.close();
    return 0;
}