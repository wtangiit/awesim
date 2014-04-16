#include "util.h"
#include "awe_types.h"
#include "lp_awe_server.h"
#include "lp_shock.h"

#include <string.h>
#include <assert.h>
#include <ross-types.h>
#include "ross.h"

#include "codes/codes.h"
#include "codes/codes_mapping.h"
#include "codes/configuration.h"
#include "codes/lp-type-lookup.h"


#define DOWNLOAD 0
#define UPLOAD 1

/* define state*/
typedef struct awe_client_state awe_client_state;
struct awe_client_state {
    char current_work[MAX_LENGTH_ID];
    int  total_processed;
    double busy_time;
    tw_stime start_ts;    /* time that we started sending requests */
    tw_stime end_ts;      /* time that last request finished */
};

/* ROSS expects four functions per LP:
 * - an LP initialization function, called for each LP
 * - an event processing function
 * - a *reverse* event processing function (rollback), and
 * - a finalization/cleanup function when the simulation ends
 */
static void lpf_awe_client_init(awe_client_state * ns, tw_lp * lp);
static void lpf_awe_client_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void lpf_awe_client_rev_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void lpf_awe_client_finalize(awe_client_state * ns, tw_lp * lp);

/*event handlers*/
static void handle_kick_off_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_timer_request_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_work_checkout_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_compute_done_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_input_downloaded_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_output_uploaded_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);

/*event planners*/
static void plan_future_event(tw_lp *lp, awe_event_type event_type, tw_stime interval, void* userdata);

/*msg senders*/
static void send_work_checkout_request(tw_lp *lp);
static void send_work_done_notification(char* work_id, tw_lp *lp);

/*data transfer*/
static void data_transfer(tw_lp *lp, char* work_id, double size, double latency, awe_event_type event_type);

/* set up the function pointers for ROSS, as well as the size of the LP state
 * structure (NOTE: ROSS is in charge of event and state (de-)allocation) */
tw_lptype awe_client_lp = {
     (init_f) lpf_awe_client_init,
     (event_f) lpf_awe_client_event,
     (revent_f) lpf_awe_client_rev_event,
     (final_f) lpf_awe_client_finalize, 
     (map_f) codes_mapping,
     sizeof(awe_client_state),
};

/*start implementations*/
const tw_lptype* awe_client_get_lp_type()
{
    return(&awe_client_lp);
}

void init_awe_client() {
    /*parse workload file and init work_map*/
    return;
}


void register_lp_awe_client()
{
    /* lp_type_register should be called exactly once per process per 
     * LP type */
    lp_type_register("awe_client", awe_client_get_lp_type());
}

void lpf_awe_client_init(
    awe_client_state * cs,
    tw_lp * lp)
{
    tw_event *e;
    awe_msg *m;
    tw_stime kickoff_time;
    
    memset(cs, 0, sizeof(*cs));
            
    /* skew each kickoff event slightly to help avoid event ties later on */
    kickoff_time = g_tw_lookahead + tw_rand_unif(lp->rng); ;

    /* first create the event (time arg is an offset, not absolute time) */
    e = codes_event_new(lp->gid, kickoff_time, lp);
    /* after event is created, grab the allocated message and set msg-specific
     * data */ 
    m = tw_event_data(e);
    m->event_type = KICK_OFF;
    /* event is ready to be processed, send it off */
    tw_event_send(e);

    return;
}

/* event processing entry point
 * - simply forward the message to the appropriate handler */
void lpf_awe_client_event(
    awe_client_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
   switch (m->event_type)
    {
        case KICK_OFF:
            handle_kick_off_event(ns, b, m, lp);
            break;
        case TIMER_REQUEST:
            handle_timer_request_event(ns, b, m, lp);
            break;
        case WORK_CHECKOUT:
            handle_work_checkout_event(ns, b, m, lp);
            break;
        case INPUT_DOWNLOADED:
            handle_input_downloaded_event(ns, b, m, lp);
            break;
        case COMPUTE_DONE:
            handle_compute_done_event(ns, b, m, lp);
            break;
        case OUTPUT_UPLOADED:
            handle_output_uploaded_event(ns, b, m, lp);
            break;
        default:
	    printf("\nawe_client Invalid message type %d from %lu\n", m->event_type, m->src);
        break;
    }
}

/* reverse event processing entry point
 * - simply forward the message to the appropriate handler */
void lpf_awe_client_rev_event(
    awe_client_state * cs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    return;
}

/* once the simulation is over, do some output */
void lpf_awe_client_finalize(
    awe_client_state * ns,
    tw_lp * lp)
{
    ns->end_ts = tw_now(lp);
    printf("[awe_client][%lu]start_time=%lf, end_time=%lf, makespan=%lf, processed=%d, busy_time=%lf, busy_rate=%lf\n",
            lp->gid,
            ns->start_ts, 
            ns->end_ts,
            ns->end_ts - ns->start_ts,
            ns->total_processed,
            ns->busy_time,
            ns->busy_time / (ns->end_ts - ns->start_ts) );
    
    return;
}

/* handle initial event (initialize job submission) */
void handle_kick_off_event(
    awe_client_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    plan_future_event(lp, TIMER_REQUEST, g_tw_lookahead, NULL);
    return;
}

void handle_timer_request_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp) {
    /*if (tw_now(lp) > finish_stime) {
        return;
    }*/
    send_work_checkout_request(lp);
    /* printf("[%lf][awe_client-gid=%lu,lid=%lu]: send checkout request to server\n", tw_now(lp), lp->gid, lp->id); */
    return;
}

/* workunit checkout -> download inpout from shock */
void handle_work_checkout_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp) {
    if (strlen(m->object_id)>0) {
        char* workid = m->object_id;
        Workunit* work = g_hash_table_lookup(work_map, workid);
        printf("[%lf][awe_client][%lu]WC;workid=%s\n", tw_now(lp), lp->gid, workid);
        data_transfer(lp, workid, work->stats.size_infile, work->stats.time_data_in, DATA_DOWNLOAD);
        ns->busy_time += work->stats.time_data_in;
        printf("[%lf][awe_client][%lu]FI;workid=%s\n", tw_now(lp), lp->gid, workid);
    }
}
/* input downloaded -> start run command*/
void handle_input_downloaded_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp) {
    if (strlen(m->object_id)>0) {
        char* workid = m->object_id;
        Workunit* work = g_hash_table_lookup(work_map, workid);
        printf("[%lf][awe_client][%lu]FD;workid=%s;size_data_in=%lu;time_data_in=%lf\n", 
                tw_now(lp), 
                lp->gid, 
                workid,
                work->stats.size_infile,
                work->stats.time_data_in);
        plan_future_event(lp, COMPUTE_DONE, work->stats.runtime, workid);
        printf("[%lf][awe_client][%lu]WS;workid=%s\n", tw_now(lp), lp->gid, workid);
    }
}

/* compute done -> upload output to shock*/
void handle_compute_done_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp) {
    char *workid = m->object_id;
    Workunit* work = g_hash_table_lookup(work_map, workid);
    printf("[%lf][awe_client][%lu]WD;workid=%s;cmd=%s;runtime=%lf\n", tw_now(lp), lp->gid, workid, work->cmd, work->stats.runtime);
    ns->busy_time += work->stats.runtime;
    data_transfer(lp, workid, work->stats.size_outfile, work->stats.time_data_out, DATA_UPLOAD);
    printf("[%lf][awe_client][%lu]FO;workid=%s\n", tw_now(lp), lp->gid, workid);
}

/* output uploaded -> notify awe-server and ask for next workunit*/
void handle_output_uploaded_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp) {
    ns->total_processed += 1;
    char *workid = m->object_id;
    Workunit* work = g_hash_table_lookup(work_map, workid);
    ns->busy_time += work->stats.time_data_out;
    printf("[%lf][awe_client][%lu]FU;workid=%s;size_data_out=%lu;time_data_out=%lf\n", 
                tw_now(lp), 
                lp->gid, 
                workid,
                work->stats.size_outfile,
                work->stats.time_data_out);
    send_work_done_notification(workid, lp);
    plan_future_event(lp, TIMER_REQUEST, g_tw_lookahead, NULL);
}

void send_work_checkout_request(tw_lp *lp) {
    tw_event *e;
    awe_msg *msg;
    tw_lpid server_id = get_awe_server_lp_id();
    e = codes_event_new(server_id, g_tw_lookahead, lp);
    msg = tw_event_data(e);
    msg->event_type = WORK_CHECKOUT;
    msg->src = lp->gid;
    char str[15];
    sprintf(str, "%lu", lp->gid);
    strcpy(msg->object_id, str);
    tw_event_send(e);
    return;
}

void send_work_done_notification(char* work_id, tw_lp *lp) {
    tw_event *e;
    awe_msg *msg;
    tw_lpid server_id = get_awe_server_lp_id();
    e = codes_event_new(server_id, g_tw_lookahead, lp);
    msg = tw_event_data(e);
    msg->event_type = WORK_DONE;
    msg->src = lp->gid;
    strcpy(msg->object_id, work_id);
    tw_event_send(e);
    return;
}

void data_transfer(tw_lp *lp, char* work_id, double size, double latency, awe_event_type event_type) {
    tw_event *e;
    awe_msg *msg;
    tw_lpid server_id = get_shock_lp_id();
    e = codes_event_new(server_id, latency, lp);
    msg = tw_event_data(e);
    msg->event_type = event_type;
    msg->src = lp->gid;
    strcpy(msg->object_id, work_id);
    msg->size =  size;
    tw_event_send(e);
    return;
}

void plan_future_event(tw_lp *lp, awe_event_type event_type, tw_stime interval, void* userdata) {
    tw_event *e;
    awe_msg *msg;
    e = codes_event_new(lp->gid, interval, lp);
    msg = tw_event_data(e);
    msg->event_type = event_type;
    if (userdata) {
        strcpy(msg->object_id, (char*)userdata);
    }
    /* event is ready to be processed, send it off */
    tw_event_send(e);
}
