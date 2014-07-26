/* 
 * File:   awe_types.h
 * Author: wtang
 *
 * Created on April 1, 2014, 3:04 PM
 */

#ifndef AWE_TYPES_H
#define	AWE_TYPES_H

#include <stdint.h>

#define MAX_LENGTH_UUID 36
#define MAX_LENGTH_ID 45
#define MAX_LENGTH_STATE 15
#define MAX_LENGTH_GROUP 20
#define MAX_NAME_LENGTH_WKLD 512
#define MAX_IO_FILE_NUM 100
#define MAX_NUM_TASKS 30

#define TIMER_CHECKOUT_INTERVAL 100



extern  GHashTable *work_map;
extern  GHashTable *job_map;

/* common event, msg types*/

/* define event*/
typedef enum awe_event_type awe_event_type;
enum awe_event_type
{
    KICK_OFF,    /* initial event */
    JOB_SUBMIT,  /*from initilized workload*/
    TASK_READY,  /*from self*/
    WORK_ENQUEUE,
    WORK_CHECKOUT, /*from client*/
    WORK_DONE, /*from client*/
    DOWNLOAD_REQUEST,  /* client -> shock, to request input*/
    INPUT_DATA_DOWNLOAD, /* shock -> client, to send input data*/
    COMPUTE_DONE,
    OUTPUT_DATA_UPLOAD, /* client -> shock, to upload output data*/
    OUTPUT_UPLOADED,  /* client -> client, local event for OUTPUT_DATA_UPLOAD*/
    /*following are data related events*/
    UPLOAD_REQ, /* client->endpoint, endpoint->shock*/
    UPLOAD_ACK, /* shock->endpoint, endpoint->client*/
    DNLOAD_REQ, /* client->endpoint, endpoint->shock*/
    DNLOAD_ACK, /* shock->endpoint, endpoint->client*/
};

typedef struct awe_msg awe_msg;
struct awe_msg {
    enum awe_event_type event_type;
    tw_lpid src;          /* source of this request or ack */
    tw_lpid next_hop;          /* for fwd msg, next hop to forward */
    tw_lpid last_hop;          /* for fwd msg, last hop before forward */
    char object_id[MAX_LENGTH_ID]; 
    uint64_t size;  /*data size*/
    int incremented_flag; /* helper for reverse computation */
};

/* end of ross common msg types*/

/* data structure for AWE elements*/
typedef struct JobStat JobStat;
struct JobStat  {
    double created;
    double start;
    double end;
};

typedef struct TaskStat TaskStat;
struct TaskStat {
    double created;
    double start;
    double end;
    long size_predata;
    long size_infile;
    long size_outfile;
};

typedef struct WorkStat WorkStat;
struct WorkStat {
    double st_created;
    double st_checkout;
    double st_download_start;
    double st_download_end;
    double st_upload_start;
    double st_upload_end;
    double time_predata_in;
    double runtime;
    double time_data_in;
    double time_data_out;
    uint64_t size_predata;
    uint64_t size_infile;
    uint64_t size_outfile;
};

typedef struct DataObj DataObj;
struct DataObj {
    char name[MAX_NAME_LENGTH_WKLD];
    char host[MAX_NAME_LENGTH_WKLD];
    char nodeid[MAX_LENGTH_ID];
    int size;
};

typedef struct Workunit Workunit;
struct Workunit {
    char id[MAX_LENGTH_ID];
    char jobid[MAX_LENGTH_ID];
    int stage;
    int rank;
    int num_inputs;
    DataObj Inputs[MAX_IO_FILE_NUM];
    int num_outputs;
    DataObj Outputs[MAX_IO_FILE_NUM];
    int num_predata;
    DataObj Predata[MAX_IO_FILE_NUM];
    char cmd[MAX_NAME_LENGTH_WKLD];
    char state[MAX_NAME_LENGTH_WKLD];
    int splits;
    int max_split_size;
    struct WorkStat stats;
};

typedef struct Task Task;
struct Task{
    char id[MAX_LENGTH_ID];
    char cmd[MAX_NAME_LENGTH_WKLD];
    char state[MAX_NAME_LENGTH_WKLD];
    int splits;
    int remain_work;
    int depend[MAX_NUM_TASKS];
    TaskStat stats;
};

typedef struct Job {
    char id[MAX_LENGTH_ID];
    char username[MAX_NAME_LENGTH_WKLD];
    char project[MAX_NAME_LENGTH_WKLD];
    char pipeline[MAX_NAME_LENGTH_WKLD];
    uint64_t inputsize;
    int num_tasks;
    int remain_tasks;
    int task_splits[MAX_NUM_TASKS];
    int task_remainwork[MAX_NUM_TASKS];
    int task_dep[MAX_NUM_TASKS][MAX_NUM_TASKS];
    int task_states[MAX_NUM_TASKS];  /* 0=pending, 1=parsed, 2=completed*/
    char state[MAX_LENGTH_STATE];
    JobStat stats;
}Job;

typedef struct Site Site;
struct Site{
    char id[MAX_LENGTH_ID];
    int num_capacity;
    int num_idle;
    int num_busy;
    int distance;
};

/* end of data structure for AWE elements*/

#endif	/* AWE_TYPES_H */


