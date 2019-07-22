/* Copyright (C) 2013 Codership Oy <info@codership.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License along
   with this program; if not, write to the Free Software Foundation, Inc.,
   51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA. */

#include "mariadb.h"
#include "wsrep_thd.h"
#include "wsrep_trans_observer.h"
#include "wsrep_high_priority_service.h"
#include "wsrep_storage_service.h"
#include "transaction.h"
#include "rpl_rli.h"
#include "log_event.h"
#include "sql_parse.h"
#include "sql_base.h" // close_thread_tables()
#include "mysqld.h"   // start_wsrep_THD();
#include "wsrep_applier.h"   // start_wsrep_THD();
#include "mysql/service_wsrep.h"
#include "debug_sync.h"
#include "slave.h"
#include "rpl_rli.h"
#include "rpl_mi.h"

static Wsrep_thd_queue* wsrep_rollback_queue= 0;
static Wsrep_thd_queue* wsrep_post_rollback_queue= 0;
static Atomic_counter<uint64_t> wsrep_bf_aborts_counter;


int wsrep_show_bf_aborts (THD *thd, SHOW_VAR *var, char *buff,
                          enum enum_var_type scope)
{
  wsrep_local_bf_aborts= wsrep_bf_aborts_counter;
  var->type= SHOW_LONGLONG;
  var->value= (char*)&wsrep_local_bf_aborts;
  return 0;
}

static void wsrep_replication_process(THD *thd,
                                      void* arg __attribute__((unused)))
{
  DBUG_ENTER("wsrep_replication_process");

  Wsrep_applier_service applier_service(thd);

  /* thd->system_thread_info.rpl_sql_info isn't initialized. */
  thd->system_thread_info.rpl_sql_info=
    new rpl_sql_thread_info(thd->wsrep_rgi->rli->mi->rpl_filter);

  WSREP_INFO("Starting applier thread %llu", thd->thread_id);
  enum wsrep::provider::status
    ret= Wsrep_server_state::get_provider().run_applier(&applier_service);

  WSREP_INFO("Applier thread exiting ret: %d thd: %llu", ret, thd->thread_id);
  mysql_mutex_lock(&LOCK_wsrep_slave_threads);
  wsrep_close_applier(thd);
  mysql_cond_broadcast(&COND_wsrep_slave_threads);
  mysql_mutex_unlock(&LOCK_wsrep_slave_threads);

  delete thd->system_thread_info.rpl_sql_info;
  delete thd->wsrep_rgi->rli->mi;
  delete thd->wsrep_rgi->rli;
  
  thd->wsrep_rgi->cleanup_after_session();
  delete thd->wsrep_rgi;
  thd->wsrep_rgi= NULL;

<<<<<<< HEAD
||||||| merged common ancestors
void wsrep_replay_transaction(THD *thd)
{
  DBUG_ENTER("wsrep_replay_transaction");
  /* checking if BF trx must be replayed */
  if (thd->wsrep_conflict_state== MUST_REPLAY) {
    DBUG_ASSERT(wsrep_thd_trx_seqno(thd));
    if (thd->wsrep_exec_mode!= REPL_RECV) {
      if (thd->get_stmt_da()->is_sent())
      {
        WSREP_ERROR("replay issue, thd has reported status already");
      }


      /*
        PS reprepare observer should have been removed already.
        open_table() will fail if we have dangling observer here.
      */
      DBUG_ASSERT(thd->m_reprepare_observer == NULL);

      struct da_shadow
      {
          enum Diagnostics_area::enum_diagnostics_status status;
          ulonglong affected_rows;
          ulonglong last_insert_id;
          char message[MYSQL_ERRMSG_SIZE];
      };
      struct da_shadow da_status;
      da_status.status= thd->get_stmt_da()->status();
      if (da_status.status == Diagnostics_area::DA_OK)
      {
        da_status.affected_rows= thd->get_stmt_da()->affected_rows();
        da_status.last_insert_id= thd->get_stmt_da()->last_insert_id();
        strmake(da_status.message,
                thd->get_stmt_da()->message(),
                sizeof(da_status.message)-1);
      }

      thd->get_stmt_da()->reset_diagnostics_area();

      thd->wsrep_conflict_state= REPLAYING;
      mysql_mutex_unlock(&thd->LOCK_thd_data);

      thd->reset_for_next_command();
      thd->reset_killed();
      close_thread_tables(thd);
      if (thd->locked_tables_mode && thd->lock)
      {
        WSREP_DEBUG("releasing table lock for replaying (%lld)",
                    (longlong) thd->thread_id);
        thd->locked_tables_list.unlock_locked_tables(thd);
        thd->variables.option_bits&= ~(OPTION_TABLE_LOCK);
      }
      thd->mdl_context.release_transactional_locks();
      /*
        Replaying will call MYSQL_START_STATEMENT when handling
        BEGIN Query_log_event so end statement must be called before
        replaying.
      */
      MYSQL_END_STATEMENT(thd->m_statement_psi, thd->get_stmt_da());
      thd->m_statement_psi= NULL;
      thd->m_digest= NULL;
      thd_proc_info(thd, "WSREP replaying trx");
      WSREP_DEBUG("replay trx: %s %lld",
                  thd->query() ? thd->query() : "void",
                  (long long)wsrep_thd_trx_seqno(thd));
      struct wsrep_thd_shadow shadow;
      wsrep_prepare_bf_thd(thd, &shadow);

      /* From trans_begin() */
      thd->variables.option_bits|= OPTION_BEGIN;
      thd->server_status|= SERVER_STATUS_IN_TRANS;

      int rcode = wsrep->replay_trx(wsrep,
                                    &thd->wsrep_ws_handle,
                                    (void *)thd);

      wsrep_return_from_bf_mode(thd, &shadow);
      if (thd->wsrep_conflict_state!= REPLAYING)
        WSREP_WARN("lost replaying mode: %d", thd->wsrep_conflict_state );

      mysql_mutex_lock(&thd->LOCK_thd_data);

      switch (rcode)
      {
      case WSREP_OK:
        thd->wsrep_conflict_state= NO_CONFLICT;
        wsrep->post_commit(wsrep, &thd->wsrep_ws_handle);
        WSREP_DEBUG("trx_replay successful for: %lld %lld",
                    (longlong) thd->thread_id, (longlong) thd->real_id);
        if (thd->get_stmt_da()->is_sent())
        {
          WSREP_WARN("replay ok, thd has reported status");
        }
        else if (thd->get_stmt_da()->is_set())
        {
          if (thd->get_stmt_da()->status() != Diagnostics_area::DA_OK &&
              thd->get_stmt_da()->status() != Diagnostics_area::DA_OK_BULK)
          {
            WSREP_WARN("replay ok, thd has error status %d",
                       thd->get_stmt_da()->status());
          }
        }
        else
        {
          if (da_status.status == Diagnostics_area::DA_OK)
          {
            my_ok(thd,
                  da_status.affected_rows,
                  da_status.last_insert_id,
                  da_status.message);
          }
          else
          {
            my_ok(thd);
          }
        }
        break;
      case WSREP_TRX_FAIL:
        if (thd->get_stmt_da()->is_sent())
        {
          WSREP_ERROR("replay failed, thd has reported status");
        }
        else
        {
          WSREP_DEBUG("replay failed, rolling back");
        }
        thd->wsrep_conflict_state= ABORTED;
        wsrep->post_rollback(wsrep, &thd->wsrep_ws_handle);
        break;
      default:
        WSREP_ERROR("trx_replay failed for: %d, schema: %s, query: %s",
                    rcode, thd->get_db(),
                    thd->query() ? thd->query() : "void");
        /* we're now in inconsistent state, must abort */

        /* http://bazaar.launchpad.net/~codership/codership-mysql/5.6/revision/3962#sql/wsrep_thd.cc */
        mysql_mutex_unlock(&thd->LOCK_thd_data);

        unireg_abort(1);
        break;
      }

      wsrep_cleanup_transaction(thd);

      mysql_mutex_lock(&LOCK_wsrep_replaying);
      wsrep_replaying--;
      WSREP_DEBUG("replaying decreased: %d, thd: %lld",
                  wsrep_replaying, (longlong) thd->thread_id);
      mysql_cond_broadcast(&COND_wsrep_replaying);
      mysql_mutex_unlock(&LOCK_wsrep_replaying);
    }
  }
  DBUG_VOID_RETURN;
}

static void wsrep_replication_process(THD *thd)
{
  int rcode;
  DBUG_ENTER("wsrep_replication_process");

  struct wsrep_thd_shadow shadow;
  wsrep_prepare_bf_thd(thd, &shadow);

  /* From trans_begin() */
  thd->variables.option_bits|= OPTION_BEGIN;
  thd->server_status|= SERVER_STATUS_IN_TRANS;

  rcode = wsrep->recv(wsrep, (void *)thd);
  DBUG_PRINT("wsrep",("wsrep_repl returned: %d", rcode));

  WSREP_INFO("applier thread exiting (code:%d)", rcode);

  switch (rcode) {
  case WSREP_OK:
  case WSREP_NOT_IMPLEMENTED:
  case WSREP_CONN_FAIL:
    /* provider does not support slave operations / disconnected from group,
     * just close applier thread */
    break;
  case WSREP_NODE_FAIL:
    /* data inconsistency => SST is needed */
    /* Note: we cannot just blindly restart replication here,
     * SST might require server restart if storage engines must be
     * initialized after SST */
    WSREP_ERROR("node consistency compromised, aborting");
    wsrep_kill_mysql(thd);
    break;
  case WSREP_WARNING:
  case WSREP_TRX_FAIL:
  case WSREP_TRX_MISSING:
    /* these suggests a bug in provider code */
    WSREP_WARN("bad return from recv() call: %d", rcode);
    /* Shut down this node. */
    /* fall through */
  case WSREP_FATAL:
    /* Cluster connectivity is lost.
     *
     * If applier was killed on purpose (KILL_CONNECTION), we
     * avoid mysql shutdown. This is because the killer will then handle
     * shutdown processing (or replication restarting)
     */
    if (thd->killed != KILL_CONNECTION)
    {
      wsrep_kill_mysql(thd);
    }
    break;
  }

  mysql_mutex_lock(&LOCK_thread_count);
  wsrep_close_applier(thd);
  mysql_cond_broadcast(&COND_thread_count);
  mysql_mutex_unlock(&LOCK_thread_count);
=======
void wsrep_replay_transaction(THD *thd)
{
  DBUG_ENTER("wsrep_replay_transaction");
  /* checking if BF trx must be replayed */
  if (thd->wsrep_conflict_state== MUST_REPLAY) {
    DBUG_ASSERT(wsrep_thd_trx_seqno(thd));
    if (thd->wsrep_exec_mode!= REPL_RECV) {
      if (thd->get_stmt_da()->is_sent())
      {
        WSREP_ERROR("replay issue, thd has reported status already");
      }


      /*
        PS reprepare observer should have been removed already.
        open_table() will fail if we have dangling observer here.
      */
      DBUG_ASSERT(thd->m_reprepare_observer == NULL);

      struct da_shadow
      {
          enum Diagnostics_area::enum_diagnostics_status status;
          ulonglong affected_rows;
          ulonglong last_insert_id;
          char message[MYSQL_ERRMSG_SIZE];
      };
      struct da_shadow da_status;
      da_status.status= thd->get_stmt_da()->status();
      if (da_status.status == Diagnostics_area::DA_OK)
      {
        da_status.affected_rows= thd->get_stmt_da()->affected_rows();
        da_status.last_insert_id= thd->get_stmt_da()->last_insert_id();
        strmake(da_status.message,
                thd->get_stmt_da()->message(),
                sizeof(da_status.message)-1);
      }

      thd->get_stmt_da()->reset_diagnostics_area();

      thd->wsrep_conflict_state= REPLAYING;
      mysql_mutex_unlock(&thd->LOCK_thd_data);

      thd->reset_for_next_command();
      thd->reset_killed();
      close_thread_tables(thd);
      if (thd->locked_tables_mode && thd->lock)
      {
        WSREP_DEBUG("releasing table lock for replaying (%lld)",
                    (longlong) thd->thread_id);
        thd->locked_tables_list.unlock_locked_tables(thd);
        thd->variables.option_bits&= ~(OPTION_TABLE_LOCK);
      }
      thd->mdl_context.release_transactional_locks();
      /*
        Replaying will call MYSQL_START_STATEMENT when handling
        BEGIN Query_log_event so end statement must be called before
        replaying.
      */
      MYSQL_END_STATEMENT(thd->m_statement_psi, thd->get_stmt_da());
      thd->m_statement_psi= NULL;
      thd->m_digest= NULL;
      thd_proc_info(thd, "WSREP replaying trx");
      WSREP_DEBUG("replay trx: %s %lld",
                  thd->query() ? thd->query() : "void",
                  (long long)wsrep_thd_trx_seqno(thd));
      struct wsrep_thd_shadow shadow;
      wsrep_prepare_bf_thd(thd, &shadow);

      /* From trans_begin() */
      thd->variables.option_bits|= OPTION_BEGIN;
      thd->server_status|= SERVER_STATUS_IN_TRANS;

      int rcode = wsrep->replay_trx(wsrep,
                                    &thd->wsrep_ws_handle,
                                    (void *)thd);

      wsrep_return_from_bf_mode(thd, &shadow);
      if (thd->wsrep_conflict_state!= REPLAYING)
        WSREP_WARN("lost replaying mode: %d", thd->wsrep_conflict_state );

      mysql_mutex_lock(&thd->LOCK_thd_data);

      switch (rcode)
      {
      case WSREP_OK:
        thd->wsrep_conflict_state= NO_CONFLICT;
        wsrep->post_commit(wsrep, &thd->wsrep_ws_handle);
        WSREP_DEBUG("trx_replay successful for: %lld %lld",
                    (longlong) thd->thread_id, (longlong) thd->real_id);
        if (thd->get_stmt_da()->is_sent())
        {
          WSREP_WARN("replay ok, thd has reported status");
        }
        else if (thd->get_stmt_da()->is_set())
        {
          if (thd->get_stmt_da()->status() != Diagnostics_area::DA_OK &&
              thd->get_stmt_da()->status() != Diagnostics_area::DA_OK_BULK)
          {
            WSREP_WARN("replay ok, thd has error status %d",
                       thd->get_stmt_da()->status());
          }
        }
        else
        {
          if (da_status.status == Diagnostics_area::DA_OK)
          {
            my_ok(thd,
                  da_status.affected_rows,
                  da_status.last_insert_id,
                  da_status.message);
          }
          else
          {
            my_ok(thd);
          }
        }
        break;
      case WSREP_TRX_FAIL:
        if (thd->get_stmt_da()->is_sent())
        {
          WSREP_ERROR("replay failed, thd has reported status");
        }
        else
        {
          WSREP_DEBUG("replay failed, rolling back");
        }
        thd->wsrep_conflict_state= ABORTED;
        wsrep->post_rollback(wsrep, &thd->wsrep_ws_handle);
        break;
      default:
        WSREP_ERROR("trx_replay failed for: %d, schema: %s, query: %s",
                    rcode, thd->get_db(),
                    thd->query() ? thd->query() : "void");
        /* we're now in inconsistent state, must abort */

        /* http://bazaar.launchpad.net/~codership/codership-mysql/5.6/revision/3962#sql/wsrep_thd.cc */
        mysql_mutex_unlock(&thd->LOCK_thd_data);

        unireg_abort(1);
        break;
      }

      wsrep_cleanup_transaction(thd);

      mysql_mutex_lock(&LOCK_wsrep_replaying);
      wsrep_replaying--;
      WSREP_DEBUG("replaying decreased: %d, thd: %lld",
                  wsrep_replaying, (longlong) thd->thread_id);
      mysql_cond_broadcast(&COND_wsrep_replaying);
      mysql_mutex_unlock(&LOCK_wsrep_replaying);
    }
  }
  DBUG_VOID_RETURN;
}

static void wsrep_replication_process(THD *thd)
{
  int rcode;
  DBUG_ENTER("wsrep_replication_process");

  struct wsrep_thd_shadow shadow;
  wsrep_prepare_bf_thd(thd, &shadow);

  /* From trans_begin() */
  thd->variables.option_bits|= OPTION_BEGIN;
  thd->server_status|= SERVER_STATUS_IN_TRANS;

  thd_proc_info(thd, "wsrep applier idle");
  rcode = wsrep->recv(wsrep, (void *)thd);
  DBUG_PRINT("wsrep",("wsrep_repl returned: %d", rcode));

  WSREP_INFO("applier thread exiting (code:%d)", rcode);

  switch (rcode) {
  case WSREP_OK:
  case WSREP_NOT_IMPLEMENTED:
  case WSREP_CONN_FAIL:
    /* provider does not support slave operations / disconnected from group,
     * just close applier thread */
    break;
  case WSREP_NODE_FAIL:
    /* data inconsistency => SST is needed */
    /* Note: we cannot just blindly restart replication here,
     * SST might require server restart if storage engines must be
     * initialized after SST */
    WSREP_ERROR("node consistency compromised, aborting");
    wsrep_kill_mysql(thd);
    break;
  case WSREP_WARNING:
  case WSREP_TRX_FAIL:
  case WSREP_TRX_MISSING:
    /* these suggests a bug in provider code */
    WSREP_WARN("bad return from recv() call: %d", rcode);
    /* Shut down this node. */
    /* fall through */
  case WSREP_FATAL:
    /* Cluster connectivity is lost.
     *
     * If applier was killed on purpose (KILL_CONNECTION), we
     * avoid mysql shutdown. This is because the killer will then handle
     * shutdown processing (or replication restarting)
     */
    if (thd->killed != KILL_CONNECTION)
    {
      wsrep_kill_mysql(thd);
    }
    break;
  }

  mysql_mutex_lock(&LOCK_thread_count);
  wsrep_close_applier(thd);
  mysql_cond_broadcast(&COND_thread_count);
  mysql_mutex_unlock(&LOCK_thread_count);
>>>>>>> origin/10.3

  if(thd->has_thd_temporary_tables())
  {
    WSREP_WARN("Applier %lld has temporary tables at exit.",
               thd->thread_id);
  }
  DBUG_VOID_RETURN;
}

<<<<<<< HEAD
static bool create_wsrep_THD(Wsrep_thd_args* args)
||||||| merged common ancestors
static bool create_wsrep_THD(wsrep_thd_processor_fun processor)
=======
static bool create_wsrep_THD(wsrep_thread_args* args)
>>>>>>> origin/10.3
{
  ulong old_wsrep_running_threads= wsrep_running_threads;
<<<<<<< HEAD
  pthread_t unused;

  bool res= pthread_create(&unused, &connection_attrib, start_wsrep_THD,
                           args);
||||||| merged common ancestors
  pthread_t unused;
  mysql_mutex_lock(&LOCK_thread_count);
  bool res= pthread_create(&unused, &connection_attrib, start_wsrep_THD,
                           (void*)processor);
=======
  mysql_mutex_lock(&LOCK_thread_count);
  bool res= pthread_create(&args->thread_id, &connection_attrib, start_wsrep_THD,
                           (void*)args);
>>>>>>> origin/10.3
  /*
    if starting a thread on server startup, wait until the this thread's THD
    is fully initialized (otherwise a THD initialization code might
    try to access a partially initialized server data structure - MDEV-8208).
  */
  mysql_mutex_lock(&LOCK_wsrep_slave_threads);
  if (!mysqld_server_initialized)
    while (old_wsrep_running_threads == wsrep_running_threads)
      mysql_cond_wait(&COND_wsrep_slave_threads, &LOCK_wsrep_slave_threads);
  mysql_mutex_unlock(&LOCK_wsrep_slave_threads);
  return res;
}

void wsrep_create_appliers(long threads)
{
  /*  Dont' start slave threads if wsrep-provider or wsrep-cluster-address
      is not set.
  */
  if (!WSREP_PROVIDER_EXISTS) 
  {
    return; 
  }

  if (!wsrep_cluster_address || wsrep_cluster_address[0]== 0)
  {
    WSREP_DEBUG("wsrep_create_appliers exit due to empty address");
    return;
  }

  long wsrep_threads=0;
<<<<<<< HEAD
  
  while (wsrep_threads++ < threads)
  {
    Wsrep_thd_args* args(new Wsrep_thd_args(wsrep_replication_process, 0));
    if (create_wsrep_THD(args))
    {
||||||| merged common ancestors
  while (wsrep_threads++ < threads) {
    if (create_wsrep_THD(wsrep_replication_process))
=======
  while (wsrep_threads++ < threads) {
    wsrep_thread_args* arg;
    if((arg = (wsrep_thread_args*)my_malloc(sizeof(wsrep_thread_args), MYF(0))) == NULL) {
      WSREP_ERROR("Can't allocate memory for wsrep replication thread %ld\n", wsrep_threads);
      assert(0);
    }

    arg->thread_type = WSREP_APPLIER_THREAD;
    arg->processor = wsrep_replication_process;

    if (create_wsrep_THD(arg)) {
>>>>>>> origin/10.3
      WSREP_WARN("Can't create thread to manage wsrep replication");
<<<<<<< HEAD
    }
||||||| merged common ancestors
=======
      my_free(arg);
      return;
    }
>>>>>>> origin/10.3
  }
}

static void wsrep_rollback_process(THD *rollbacker,
                                   void *arg __attribute__((unused)))
{
  DBUG_ENTER("wsrep_rollback_process");

  THD* thd= NULL;
  DBUG_ASSERT(!wsrep_rollback_queue);
  wsrep_rollback_queue= new Wsrep_thd_queue(rollbacker);
  WSREP_INFO("Starting rollbacker thread %llu", rollbacker->thread_id);

  thd_proc_info(rollbacker, "wsrep aborter idle");
  while ((thd= wsrep_rollback_queue->pop_front()) != NULL)
  {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    wsrep::client_state& cs(thd->wsrep_cs());
    const wsrep::transaction& tx(cs.transaction());
    if (tx.state() == wsrep::transaction::s_aborted)
    {
      WSREP_DEBUG("rollbacker thd already aborted: %llu state: %d",
                  (long long)thd->real_id,
                  tx.state());

      mysql_mutex_unlock(&thd->LOCK_thd_data);
      continue;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);

    thd_proc_info(rollbacker, "wsrep aborter active");

    wsrep::transaction_id transaction_id(thd->wsrep_trx().id());
    if (thd->wsrep_trx().is_streaming() &&
        thd->wsrep_trx().bf_aborted_in_total_order())
    {
      thd->store_globals();
      thd->wsrep_cs().store_globals();
      if (thd->wsrep_cs().mode() == wsrep::client_state::m_high_priority)
      {
        DBUG_ASSERT(thd->wsrep_applier_service);
        thd->wsrep_applier_service->rollback(wsrep::ws_handle(),
                                             wsrep::ws_meta());
        thd->wsrep_applier_service->after_apply();
        /* Will free THD */
        Wsrep_server_state::instance().server_service().
          release_high_priority_service(thd->wsrep_applier_service);
      }
      else
      {
        mysql_mutex_lock(&thd->LOCK_thd_data);
        /* prepare THD for rollback processing */
        thd->reset_for_next_command(true);
        thd->lex->sql_command= SQLCOM_ROLLBACK;
        mysql_mutex_unlock(&thd->LOCK_thd_data);
        /* Perform a client rollback, restore globals and signal
           the victim only when all the resources have been
           released */
        thd->wsrep_cs().client_service().bf_rollback();
        thd->reset_globals();
        thd->wsrep_cs().sync_rollback_complete();
      }
    }
    else if (wsrep_thd_is_applying(thd))
    {
      WSREP_DEBUG("rollbacker aborting SR thd: (%lld %llu)",
                  thd->thread_id, (long long)thd->real_id);
      DBUG_ASSERT(thd->wsrep_cs().mode() == Wsrep_client_state::m_high_priority);
      /* Must be streaming and must have been removed from the
         server state streaming appliers map. */
      DBUG_ASSERT(thd->wsrep_trx().is_streaming());
      DBUG_ASSERT(!Wsrep_server_state::instance().find_streaming_applier(
                     thd->wsrep_trx().server_id(),
                     thd->wsrep_trx().id()));
      DBUG_ASSERT(thd->wsrep_applier_service);

      /* Fragment removal should happen before rollback to make
         the transaction non-observable in SR table after the rollback
         completes. For correctness the order does not matter here,
         but currently it is mandated by checks in some MTR tests. */
      Wsrep_storage_service* storage_service=
        static_cast<Wsrep_storage_service*>(
          Wsrep_server_state::instance().server_service().storage_service(
            *thd->wsrep_applier_service));
      storage_service->store_globals();
      storage_service->adopt_transaction(thd->wsrep_trx());
      storage_service->remove_fragments();
      storage_service->commit(wsrep::ws_handle(transaction_id, 0),
                              wsrep::ws_meta());
      Wsrep_server_state::instance().server_service().release_storage_service(storage_service);
      thd->store_globals();
      thd->wsrep_cs().store_globals();
      thd->wsrep_applier_service->rollback(wsrep::ws_handle(),
                                           wsrep::ws_meta());
      thd->wsrep_applier_service->after_apply();
      /* Will free THD */
      Wsrep_server_state::instance().server_service()
        .release_high_priority_service(thd->wsrep_applier_service);

    }
    else
    {
      if (thd->wsrep_trx().is_streaming())
      {
        Wsrep_storage_service* storage_service=
          static_cast<Wsrep_storage_service*>(
            Wsrep_server_state::instance().server_service().
            storage_service(thd->wsrep_cs().client_service()));

        storage_service->store_globals();
        storage_service->adopt_transaction(thd->wsrep_trx());
        storage_service->remove_fragments();
        storage_service->commit(wsrep::ws_handle(transaction_id, 0),
                                wsrep::ws_meta());
        Wsrep_server_state::instance().server_service().
          release_storage_service(storage_service);
      }
      thd->store_globals();
      thd->wsrep_cs().store_globals();
      mysql_mutex_lock(&thd->LOCK_thd_data);
      /* prepare THD for rollback processing */
      thd->reset_for_next_command();
      thd->lex->sql_command= SQLCOM_ROLLBACK;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      /* Perform a client rollback, restore globals and signal
         the victim only when all the resources have been
         released */
      thd->wsrep_cs().client_service().bf_rollback();
      thd->reset_globals();
      thd->wsrep_cs().sync_rollback_complete();
      WSREP_DEBUG("rollbacker aborted thd: (%llu %llu)",
                  thd->thread_id, (long long)thd->real_id);
    }

    thd_proc_info(rollbacker, "wsrep aborter idle");
  }
  
  delete wsrep_rollback_queue;
  wsrep_rollback_queue= NULL;

  WSREP_INFO("rollbacker thread exiting %llu", rollbacker->thread_id);

  DBUG_ASSERT(rollbacker->killed != NOT_KILLED);
  DBUG_PRINT("wsrep",("wsrep rollbacker thread exiting"));
  DBUG_VOID_RETURN;
}

static void wsrep_post_rollback_process(THD *post_rollbacker,
                                        void *arg __attribute__((unused)))
{
<<<<<<< HEAD
  DBUG_ENTER("wsrep_post_rollback_process");
  THD* thd= NULL;
||||||| merged common ancestors
  if (wsrep_provider && strcasecmp(wsrep_provider, "none"))
  {
    /* create rollbacker */
    if (create_wsrep_THD(wsrep_rollback_process))
      WSREP_WARN("Can't create thread to manage wsrep rollback");
  }
}
=======
  if (wsrep_provider && strcasecmp(wsrep_provider, "none"))
  {
    wsrep_thread_args* arg;
    if((arg = (wsrep_thread_args*)my_malloc(sizeof(wsrep_thread_args), MYF(0))) == NULL) {
      WSREP_ERROR("Can't allocate memory for wsrep rollbacker thread\n");
      assert(0);
    }

    arg->thread_type = WSREP_ROLLBACKER_THREAD;
    arg->processor = wsrep_rollback_process;

    /* create rollbacker */
    if (create_wsrep_THD(arg)) {
      WSREP_WARN("Can't create thread to manage wsrep rollback");
      my_free(arg);
      return;
    }
  }
}
>>>>>>> origin/10.3

  WSREP_INFO("Starting post rollbacker thread %llu", post_rollbacker->thread_id);
  DBUG_ASSERT(!wsrep_post_rollback_queue);
  wsrep_post_rollback_queue= new Wsrep_thd_queue(post_rollbacker);

  while ((thd= wsrep_post_rollback_queue->pop_front()) != NULL)
  {
    thd->store_globals();
    wsrep::client_state& cs(thd->wsrep_cs());
    mysql_mutex_lock(&thd->LOCK_thd_data);
    DBUG_ASSERT(thd->wsrep_trx().state() == wsrep::transaction::s_aborting);
    WSREP_DEBUG("post rollbacker calling post rollback for thd %llu, conf %s",
                thd->thread_id, wsrep_thd_transaction_state_str(thd));

    cs.after_rollback();
    DBUG_ASSERT(thd->wsrep_trx().state() == wsrep::transaction::s_aborted);
    mysql_mutex_unlock(&thd->LOCK_thd_data);
  }

  delete wsrep_post_rollback_queue;
  wsrep_post_rollback_queue= NULL;

  DBUG_ASSERT(post_rollbacker->killed != NOT_KILLED);
  DBUG_PRINT("wsrep",("wsrep post rollbacker thread exiting"));
  WSREP_INFO("post rollbacker thread exiting %llu", post_rollbacker->thread_id);
  DBUG_VOID_RETURN;
}

void wsrep_create_rollbacker()
{
  if (wsrep_cluster_address && wsrep_cluster_address[0] != 0)
  {
    Wsrep_thd_args* args= new Wsrep_thd_args(wsrep_rollback_process, 0);

    /* create rollbacker */
    if (create_wsrep_THD(args))
      WSREP_WARN("Can't create thread to manage wsrep rollback");

    /* create post_rollbacker */
    args= new Wsrep_thd_args(wsrep_post_rollback_process, 0);
    if (create_wsrep_THD(args))
      WSREP_WARN("Can't create thread to manage wsrep post rollback");
   }
}

/*
  Start async rollback process

  Asserts thd->LOCK_thd_data ownership
 */
void wsrep_fire_rollbacker(THD *thd)
{
  DBUG_ASSERT(thd->wsrep_trx().state() == wsrep::transaction::s_aborting);
  DBUG_PRINT("wsrep",("enqueuing trx abort for %llu", thd->thread_id));
  WSREP_DEBUG("enqueuing trx abort for (%llu)", thd->thread_id);
  if (wsrep_rollback_queue->push_back(thd))
  {
    WSREP_WARN("duplicate thd %llu for rollbacker",
               thd->thread_id);
  }
}


int wsrep_abort_thd(void *bf_thd_ptr, void *victim_thd_ptr, my_bool signal)
{
  DBUG_ENTER("wsrep_abort_thd");
  THD *victim_thd= (THD *) victim_thd_ptr;
  THD *bf_thd= (THD *) bf_thd_ptr;
  mysql_mutex_lock(&victim_thd->LOCK_thd_data);
  if ( (WSREP(bf_thd) ||
         ( (WSREP_ON || bf_thd->variables.wsrep_OSU_method == WSREP_OSU_RSU) &&
           wsrep_thd_is_toi(bf_thd)) )                         &&
       victim_thd &&
       !wsrep_thd_is_aborting(victim_thd))
  {
      WSREP_DEBUG("wsrep_abort_thd, by: %llu, victim: %llu", (bf_thd) ?
                  (long long)bf_thd->real_id : 0, (long long)victim_thd->real_id);
      mysql_mutex_unlock(&victim_thd->LOCK_thd_data);
      ha_abort_transaction(bf_thd, victim_thd, signal);
      mysql_mutex_lock(&victim_thd->LOCK_thd_data);
  }
  else
  {
    WSREP_DEBUG("wsrep_abort_thd not effective: %p %p", bf_thd, victim_thd);
  }
  mysql_mutex_unlock(&victim_thd->LOCK_thd_data);
  DBUG_RETURN(1);
}

bool wsrep_bf_abort(const THD* bf_thd, THD* victim_thd)
{
  WSREP_LOG_THD((THD*)bf_thd, "BF aborter before");
  WSREP_LOG_THD(victim_thd, "victim before");
  wsrep::seqno bf_seqno(bf_thd->wsrep_trx().ws_meta().seqno());

  if (WSREP(victim_thd) && !victim_thd->wsrep_trx().active())
  {
    WSREP_DEBUG("wsrep_bf_abort, BF abort for non active transaction");
    wsrep_start_transaction(victim_thd, victim_thd->wsrep_next_trx_id());
  }

  bool ret;
  if (wsrep_thd_is_toi(bf_thd))
  {
    ret= victim_thd->wsrep_cs().total_order_bf_abort(bf_seqno);
  }
  else
  {
    ret= victim_thd->wsrep_cs().bf_abort(bf_seqno);
  }
  if (ret)
  {
    wsrep_bf_aborts_counter++;
  }
  return ret;
}

/*
  Get auto increment variables for THD. Use global settings for
  applier threads.
 */
void wsrep_thd_auto_increment_variables(THD* thd,
                                        unsigned long long* offset,
                                        unsigned long long* increment)
{
  if (wsrep_thd_is_applying(thd) &&
      thd->wsrep_trx().state() != wsrep::transaction::s_replaying)
  {
    *offset= global_system_variables.auto_increment_offset;
    *increment= global_system_variables.auto_increment_increment;
    return;
  }
  *offset= thd->variables.auto_increment_offset;
  *increment= thd->variables.auto_increment_increment;
}
