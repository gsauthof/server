#include "tpool_structs.h"
#include <stdlib.h>
#include <tpool.h>
#include <windows.h>

namespace tpool
{
class tpool_win_aio : public aio
{
public:
  cache<win_aio_cb> m_cache;
  PTP_CALLBACK_ENVIRON m_env;

  tpool_win_aio(PTP_CALLBACK_ENVIRON env, int max_io)
      : m_env(env), m_cache(max_io)
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
  PTP_CALLBACK_ENVIRON m_env;
  aio *m_aio;

public:
  tpool_win(PTP_CALLBACK_ENVIRON env) : m_env(env), m_aio(0){};

  virtual void submit(const task *tasks, int size) override
  {
    for (auto i= 0; i < size; i++)
    {
      if (!TrySubmitThreadpoolCallback(tasks[i].m_func, tasks[i].m_arg, m_env))
        abort();
    }
  }

  virtual aio *create_native_aio(int max_io) override
  {
    return new tpool_win_aio(m_env, max_io);
  }
};

tpool *create_tpool_win() { return new tpool_win(0); }
} // namespace tpool
