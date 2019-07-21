#include "tpool_structs.h"

#include <stdlib.h>
#include <signal.h>
#include <assert.h>
#include "tpool.h"
#include <thread>

namespace tpool
{
#ifdef LINUX_NATIVE_AIO
#include <libaio.h>
struct linux_iocb : iocb
{
  aiocb m_aiocb;
  int m_ret_len;
  int m_err;
};

class aio_linux : public aio
{
  int m_max_io_count;
  tpool* m_pool;
  io_context_t m_io_ctx;
  cache<linux_iocb> m_cache;
  bool m_in_shutdown;
  std::thread m_getevent_thread;

  static void execute_io_completion(void* param)
  {
    linux_iocb* cb = (linux_iocb*)param;
    aio_linux* aio = (aio_linux*)cb->m_aiocb.m_internal;
    cb->m_aiocb.m_callback(&cb->m_aiocb, cb->m_ret_len, cb->m_err);
    aio->m_cache.put(cb);
  }

  static void getevent_thread_routine(aio_linux* aio)
  {
    for (;;)
    {
      io_event event;
      int ret = io_getevents(aio->m_io_ctx, 1, 1, &event, nullptr);

      if (aio->m_in_shutdown)
        break;

      if (ret > 0)
      {
        linux_iocb* iocb = (linux_iocb*)event.obj;
        long long res = event.res;
        if (res < 0)
        {
          iocb->m_err = -res;
          iocb->m_ret_len = 0;
        }
        else
        {
          iocb->m_ret_len = ret;
          iocb->m_err = 0;
        }

        aio->m_pool->submit({ execute_io_completion, iocb });
        continue;
      }
      switch (ret)
      {
      case -EAGAIN:
        usleep(1000);
        continue;
      case -EINTR:
      case 0:
        continue;
      default:
        fprintf(stderr, "io_getevents returned %d\n", ret);
        abort();
      }
    }
  }

public:
  aio_linux(io_context_t ctx, tpool* pool, size_t max_count)
    : m_max_io_count(max_count), m_pool(pool), m_io_ctx(ctx),
    m_cache(max_count), m_getevent_thread(getevent_thread_routine, this)
  {
  }

  ~aio_linux()
  {
    m_in_shutdown = true;
    io_destroy(m_io_ctx);
    m_getevent_thread.join();
  }

  // Inherited via aio
  virtual int submit_aio(const aiocb* aiocb) override
  {
    linux_iocb* cb = m_cache.get();
    memcpy(&cb->m_aiocb, aiocb, sizeof(*aiocb));

    if (aiocb->m_opcode == AIO_PREAD)
      io_prep_pread(cb, aiocb->m_fh, aiocb->m_buffer, aiocb->m_len,
        aiocb->m_offset);
    else
      io_prep_pwrite(cb, aiocb->m_fh, aiocb->m_buffer, aiocb->m_len,
        aiocb->m_offset);
    cb->m_aiocb.m_internal = this;

    int ret;
    ret = io_submit(m_io_ctx, 1, (iocb * *)& cb);
    if (ret == 1)
      return 0;
    errno = -ret;
    return -1;
  }

  // Inherited via aio
  virtual int bind(native_file_handle& fd) override
  {
    return 0;
  }
  virtual int unbind(const native_file_handle& fd) override
  {
    return 0;
  }
};

aio* create_linux_aio(tpool* pool, int max_io)
{
  io_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  int ret = io_setup(max_io, &ctx);
  if (ret)
  {
    fprintf(stderr, "io_setup(%d) returned %d\n", max_io, ret);
    return nullptr;
  }
  return new aio_linux(ctx, pool, max_io);
}
#else
aio* create_linux_aio(tpool* tpool, int max_aio)
{
  return nullptr;
}
#endif
}
