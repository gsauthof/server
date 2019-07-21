#include "tpool_structs.h"
#include <stdlib.h>
#include <tpool.h>
#include <windows.h>

namespace tpool
{
class tpool_win_aio : public aio
{
  PTP_CALLBACK_ENVIRON m_env;
  cache<win_aio_cb> m_cache;
public:

  tpool_win_aio(PTP_CALLBACK_ENVIRON env, int max_io)
      :m_env(env), m_cache(max_io)
  {
  }

  virtual int submit_aio(const aiocb *aiocb) override
  {
    win_aio_cb *cb= m_cache.get();
    memset(cb, 0, sizeof(OVERLAPPED));
    cb->m_aiocb= *aiocb;
    cb->m_aiocb.m_internal= this;

    ULARGE_INTEGER uli;
    uli.QuadPart= aiocb->m_offset;
    cb->Offset= uli.LowPart;
    cb->OffsetHigh= uli.HighPart;

    StartThreadpoolIo(aiocb->m_fh.m_ptp_io);

    BOOL ok;
    if (aiocb->m_opcode == AIO_PREAD)
      ok= ReadFile(aiocb->m_fh.m_handle, aiocb->m_buffer, aiocb->m_len, 0, cb);
    else
      ok= WriteFile(aiocb->m_fh.m_handle, aiocb->m_buffer, aiocb->m_len, 0, cb);

    if (ok || (GetLastError() == ERROR_IO_PENDING))
      return 0;

    CancelThreadpoolIo(aiocb->m_fh.m_ptp_io);
    return -1;
  }

  static void CALLBACK io_completion_callback(PTP_CALLBACK_INSTANCE instance,
                                              PVOID context, PVOID overlapped,
                                              ULONG io_result, ULONG_PTR nbytes,
                                              PTP_IO io)
  {
    win_aio_cb *cb= (win_aio_cb *) overlapped;

    cb->m_aiocb.m_callback(&cb->m_aiocb, (int) nbytes, (int) io_result);

    tpool_win_aio *aio= (tpool_win_aio *) cb->m_aiocb.m_internal;
    aio->m_cache.put(cb);
  }

  // Inherited via aio
  virtual int bind(native_file_handle &fd) override
  {
    fd.m_ptp_io=
        CreateThreadpoolIo(fd.m_handle, io_completion_callback, 0, m_env);
    if (fd.m_ptp_io)
      return 0;
    return -1;
  }
  virtual int unbind(const native_file_handle &fd) override
  {
    if (fd.m_ptp_io)
      CloseThreadpoolIo(fd.m_ptp_io);
    return 0;
  }
};

class tpool_win : public tpool
{
  PTP_POOL m_pool;
  TP_CALLBACK_ENVIRON m_env;
  PTP_CLEANUP_GROUP m_cleanup;
  const int TASK_CACHE_SIZE=10000;

  struct task_cache_entry
  {
    cache<task_cache_entry> *m_cache;
    task m_task;
  };
  cache<task_cache_entry> m_task_cache;

public:
  tpool_win(int min_threads = 0, int max_threads = 0):m_task_cache(TASK_CACHE_SIZE)
  {
    InitializeThreadpoolEnvironment(&m_env);
    m_pool = CreateThreadpool(NULL);
    m_cleanup = CreateThreadpoolCleanupGroup();
    SetThreadpoolCallbackPool(&m_env, m_pool);
    SetThreadpoolCallbackCleanupGroup(&m_env, m_cleanup, 0);
    if (min_threads)
      SetThreadpoolThreadMinimum(m_pool, min_threads);
    if (max_threads)
     SetThreadpoolThreadMaximum(m_pool, max_threads);
  }
  ~tpool_win()
  {
    CloseThreadpoolCleanupGroupMembers(m_cleanup, FALSE, NULL);
    CloseThreadpoolCleanupGroup(m_cleanup);
    CloseThreadpool(m_pool);
  }
  static void CALLBACK task_callback(PTP_CALLBACK_INSTANCE, void *param)
  {
    auto entry = (task_cache_entry *)param;
    auto task= entry->m_task;
    entry->m_cache->put(entry);

    task.m_func(task.m_arg);
  }
  virtual void submit(const task &task) override
  {
    auto entry = m_task_cache.get();
    entry->m_cache = &m_task_cache;
    entry->m_task = task;
    if (!TrySubmitThreadpoolCallback(task_callback, entry, &m_env))
        abort();
  }

  virtual aio *create_native_aio(int max_io) override
  {
    return new tpool_win_aio(&m_env,max_io);
  }
};

tpool *create_tpool_win(int min_threads, int max_threads)
{ 
  return new tpool_win(min_threads, max_threads); 
}
} // namespace tpool
