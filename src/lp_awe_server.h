/* 
 * File:   lp_awe_svr.h
 * Author: wtang
 *
 * Created on April 1, 2014, 11:11 PM
 */

#ifndef LP_AWE_SVR_H
#define	LP_AWE_SVR_H

#include "glib.h"
#include "ross.h"


extern void init_awe_server();
extern void register_lp_awe_server();
extern tw_lpid get_awe_server_lp_id();

extern int sched_policy; //0: round-robin, 1: data-aware-best-fit, 2: data-aware-greedy

#endif	/* LP_AWE_SVR_H */

