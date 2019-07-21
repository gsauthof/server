#include <mutex>
#ifndef _WIN32
#include <unistd.h> /* pread(), pwrite() */
#endif
#include <string.h>
#include "tpool.h"
#include "tpool_structs.h"
#include <stdlib.h>

namespace tpool
{ 
#ifdef _WIN32
struct sync_io_event
{
  HANDLE m_event;
  sync_io_event()
  {
    m_event = CreateEvent(0, FALSE, FALSE, 0);
    m_event = (HANDLE)(((uintptr_t)m_event)|1);
  }
  ~sync_io_event()
  {
    m_event = (HANDLE)(((uintptr_t)m_event) & ~1);
    CloseHandle(m_event);
  }
};
static thread_local sync_io_event sync_event;

static int pread(const native_file_handle& h, void* buf, size_t count, unsigned long long offset)
{
  OVERLAPPED ov{};
  ULARGE_INTEGER uli;
  uli.QuadPart = offset;
  ov.Offset = uli.LowPart;
  ov.OffsetHigh = uli.HighPart;
  ov.hEvent = sync_event.m_event;

  if (ReadFile(h, buf, (DWORD)count, 0, &ov)
     || (GetLastError() == ERROR_IO_PENDING))
  {
    DWORD n_bytes;
    if (GetOverlappedResult(h, &ov, &n_bytes, TRUE))
      return n_bytes;
  }

  return -1;
}

static int pwrite(const native_file_handle& h, void* buf, size_t count, unsigned long long offset)
{
  OVERLAPPED ov{};
  ULARGE_INTEGER uli;
  uli.QuadPart = offset;
  ov.Offset = uli.LowPart;
  ov.OffsetHigh = uli.HighPart;
  ov.hEvent = sync_event.m_event;

  if (WriteFile(h, buf, (DWORD)count, 0, &ov) 
     || (GetLastError() == ERROR_IO_PENDING))
  {
    DWORD n_bytes;
    if (GetOverlappedResult(h, &ov, &n_bytes, TRUE))
      return n_bytes;
  }
  return -1;
}
#endif

class simulated_aio: public aio
{
  tpool* m_pool; 
  cache<aiocb> m_cache;

public:
  simulated_aio(tpool* tp, int max_io_size):
    m_pool(tp), m_cache(max_io_size, NOTIFY_ONE)
  {
  }
 
  static void simulated_aio_callback(void* param)
  {
    aiocb* cb= (aiocb*)param;
    int ret_len;
    int err = 0;
    switch (cb->m_opcode)
    {
      case AIO_PREAD:
        ret_len = pread(cb->m_fh, cb->m_buffer, cb->m_len, cb->m_offset);
        break;
      case AIO_PWRITE:
        ret_len = pwrite(cb->m_fh, cb->m_buffer, cb->m_len, cb->m_offset);
        break;
      default:
        abort();
    }
    if (ret_len  < 0)
    {
#ifdef _WIN32
      err = GetLastError();
#else
      err = errno;
#endif  
    }

    cb->m_callback(cb, ret_len, err);
    ((simulated_aio *)cb->m_internal)->m_cache.put(cb);
  }

  virtual int submit_aio(const aiocb *aiocb) override
  {
    auto cb =  m_cache.get();
    *cb = *aiocb;
    cb->m_internal = this;
    m_pool->submit({simulated_aio_callback,cb});
    return 0;
  }
  virtual int bind(native_file_handle& fd) override
  {
    return 0;
  }
  virtual int unbind(const native_file_handle& fd) override
  {
    return 0;
  }
};

aio* create_simulated_aio(tpool* tp, int max_aio)
{
  return new simulated_aio(tp,max_aio);
}

}