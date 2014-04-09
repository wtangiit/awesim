/* 
 * File:   awe_types.h
 * Author: wtang
 *
 * Created on April 1, 2014, 3:04 PM
 */

#ifndef AWE_TYPES_H
#define	AWE_TYPES_H

#define MAX_LENGTH_UUID 36
#define MAX_LENGTH_ID 45
#define MAX_NAME_LENGTH_WKLD 512
#define MAX_IO_FILE_NUM 100

#define TIMER_CHECKOUT_INTERVAL 100

/* common event, msg types*/

/* define event*/
typedef enum awe_event_type awe_event_type;
enum awe_event_type
{
    KICK_OFF,    /* initial event */
    JOB_SUBMIT,  /*from initilized workload*/
    TASK_READY,  /*from self*/
    WORK_CHECKOUT, /*from client*/
    WORK_DONE, /*from client*/
    TIMER_REQUEST,  /*from self timer*/
};

typedef struct awe_msg awe_msg;
struct awe_msg {
    enum awe_event_type event_type;
    tw_lpid src;          /* source of this request or ack */
    char object_id[MAX_LENGTH_ID];
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
    double size_predata;
    double size_infile;
    double size_outfile;
};

typedef struct WorkStat WorkStat;
struct WorkStat {
    double created;
    double done;
    double checkout;
    double deliver;
    double time_predata_in;
    double runtime;
    double time_data_in;
    double time_data_out;
    double size_predata;
    double size_infile;
    double size_outfile;
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
    int remain_work;
    TaskStat stats;
};

typedef struct Job {
    char id[MAX_LENGTH_ID];
    char username[MAX_NAME_LENGTH_WKLD];
    char project[MAX_NAME_LENGTH_WKLD];
    char pipeline[MAX_NAME_LENGTH_WKLD];
    double create_time;
    int num_task;
    Task Tasks[20];
    char *state;
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


