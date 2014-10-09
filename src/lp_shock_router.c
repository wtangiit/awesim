#include <string.h>
#include <assert.h>
#include "ross.h"
#include "glib.h"

#include "lp_shock_router.h"
#include "util.h"
#include "awe_types.h"

#include "codes/model-net.h"
#include "codes/codes.h"
#include "codes/codes_mapping.h"
#include "codes/configuration.h"
#include "codes/lp-type-lookup.h"


/* define state*/
typedef struct shock_router_state shock_router_state;
/* this struct serves as the ***persistent*** state of the LP representing the
 * server in question. This struct is setup when the LP initialization function
 * ptr is called */
struct shock_router_state {
    long size_download;
    long size_upload;
    double time_download;
    double time_upload;
    tw_stime start_ts;    /* time that we started sending requests */
    tw_stime end_ts;      /* time that last request finished */
};


/* ROSS expects four functions per LP:
 * - an LP initialization function, called for each LP
 * - an event processing function
 * - a *reverse* event processing function (rollback), and
 * - a finalization/cleanup function when the simulation ends
 */
static void lpf_shock_router_init(shock_router_state * ns, tw_lp * lp);
static void lpf_shock_router_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void lpf_shock_router_rev_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void lpf_shock_router_finalize(shock_router_state * ns, tw_lp * lp);

/*event handlers*/
static void handle_kick_off_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_data_upload_req_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_data_upload_ack_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_data_download_req_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_data_download_ack_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);

/* set up the function pointers for ROSS, as well as the size of the LP state
 * structure (NOTE: ROSS is in charge of event and state (de-)allocation) */
tw_lptype shock_router_lp = {
     (init_f) lpf_shock_router_init,
     (pre_run_f) NULL,
     (event_f) lpf_shock_router_event,
     (revent_f) lpf_shock_router_rev_event,
     (final_f) lpf_shock_router_finalize,
     (map_f) codes_mapping,
     sizeof(shock_router_state),
};

/*start implementations*/

const tw_lptype* shock_router_get_lp_type()
{
    return(&shock_router_lp);
}

void register_lp_shock_router()
{
    /* lp_type_register should be called exactly once per process per 
     * LP type */
    lp_type_register("shock_router", shock_router_get_lp_type());
}

tw_lpid get_shock_router_lp_id() {
    tw_lpid rtn_id;
    codes_mapping_get_lp_id("SHOCK_ROUTER", "shock_router", NULL, 1, 0, 0, &rtn_id);
    return rtn_id;
}

void lpf_shock_router_init(
	shock_router_state * ns,
    tw_lp * lp)
{
    tw_event *e;
    awe_msg *m;
    tw_stime kickoff_time;
    
    memset(ns, 0, sizeof(*ns));
    /* skew each kickoff event slightly to help avoid event ties later on */
    kickoff_time = 0.00;
    /* first create the event (time arg is an offset, not absolute time) */
    e = codes_event_new(lp->gid, kickoff_time, lp);
    /* after event is created, grab the allocated message and set msg-specific
     * data */ 
    m = tw_event_data(e);
    m->event_type = KICK_OFF;
    m->src = lp->gid;
    /* event is ready to be processed, send it off */
    tw_event_send(e);
    return;
}

/* event processing entry point
 * - simply forward the message to the appropriate handler */
void lpf_shock_router_event(
    shock_router_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    switch (m->event_type)
    {
        case KICK_OFF:
           handle_kick_off_event(ns, b, m, lp);
           break;
        case UPLOAD_REQ:
            handle_data_upload_req_event(ns, b, m, lp);
            break;
        case UPLOAD_ACK:
            handle_data_upload_ack_event(ns, b, m, lp);
            break;
        case DNLOAD_REQ:
            handle_data_download_req_event(ns, b, m, lp);
            break;
        case DNLOAD_ACK:
            handle_data_download_ack_event(ns, b, m, lp);
            break;
        default:
	    printf("\n shock_router Invalid message type %d \n", m->event_type);
        break;
    }
}

/* reverse event processing entry point
 * - simply forward the message to the appropriate handler */
void lpf_shock_router_rev_event(
    shock_router_state * qs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    return;
}

/* once the simulation is over, do some output */
void lpf_shock_router_finalize(
    shock_router_state * ns,
    tw_lp * lp)
{
    ns->end_ts = tw_now(lp);
    printf("[shock_router][%lu]start_time=%lf;end_time=%lf, makespan=%lf, data_download_size=%lu, data_upload_size=%lu\n",
        lp->gid,
        ns->start_ts, 
        ns->end_ts,
        ns->end_ts - ns->start_ts,
        ns->size_download,
        ns->size_upload
        );
    return;
}



/* handle initial event (initialize job submission) */
void handle_kick_off_event(
    shock_router_state * qs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    printf("[%lf][shock_router][%lu]Start serving...\n", now_sec(lp), lp->gid);
    return;
}

void handle_data_upload_req_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
void handle_data_upload_ack_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
void handle_data_download_req_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
void handle_data_download_ack_event(shock_router_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);

void handle_data_download_req_event(
    shock_router_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    tw_event *e;
    awe_msg *msg;
    tw_lpid dest_id = m->next_hop;
    e = codes_event_new(dest_id, ns_tw_lookahead, lp);
    msg = tw_event_data(e);
    msg->event_type = DNLOAD_REQ;
    msg->src = lp->gid;
    msg->last_hop = m->src;
    msg->size = m->size;
    strcpy(msg->object_id, m->object_id);
    tw_event_send(e);
    return;
}

void handle_data_download_ack_event(
    shock_router_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    awe_msg m_remote;

    tw_lpid dest_id = m->next_hop;
    
    m_remote.event_type = DNLOAD_ACK;
    m_remote.src = lp->gid;
    strcpy(m_remote.object_id, m->object_id);
    m_remote.size = m->size;

    //printf("[%lf][shock_router][%lu][StartSending]client=%lu;filesize=%llu\n", now_sec(lp), lp->gid, m->src, m->size);

    model_net_event(net_id, "download", dest_id, m->size, 0.0, sizeof(awe_msg), (const void*)&m_remote, 0, NULL, lp);
    ns->size_download += m->size;
    return;
}

void handle_data_upload_req_event(
    shock_router_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    awe_msg m_remote;
    tw_lpid dest_id = m->next_hop;

    m_remote.event_type = UPLOAD_REQ;
    m_remote.src = lp->gid;
    m_remote.last_hop = m->src;
    strcpy(m_remote.object_id, m->object_id);
    m_remote.size = m->size;

    //printf("[%lf][shock_router][%lu][StartSending]client=%lu;filesize=%llu\n", now_sec(lp), lp->gid, m->src, m->size);

    model_net_event(net_id, "upload", dest_id, m->size, 0.0, sizeof(awe_msg), (const void*)&m_remote, 0, NULL, lp);
    ns->size_download += m->size;
}

void handle_data_upload_ack_event(
    shock_router_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
	//printf("[%lf][shock_router][%lu][Received]client=%lu;filesize=%llu...\n",  now_sec(lp), lp->gid, m->src, m->size);
    ns->size_upload += m->size;
    tw_event *e;
    awe_msg *msg;
    tw_lpid dest_id = m->next_hop;
    e = codes_event_new(dest_id, ns_tw_lookahead, lp);
    msg = tw_event_data(e);
    msg->event_type = UPLOAD_ACK;
    msg->src = lp->gid;
    msg->size = m->size;
    strcpy(msg->object_id, m->object_id);
    tw_event_send(e);
    return;
}
