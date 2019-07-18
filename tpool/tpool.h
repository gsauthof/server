#pragma once
#ifdef _WIN32
#ifndef NOMINMAX  
#define NOMINMAX
#endif
#include <windows.h>
struct native_file_handle
{
  HANDLE  m_handle;
  PTP_IO  m_ptp_io;
  native_file_handle() {};
  native_file_handle(HANDLE h) :m_handle(h), m_ptp_io() {}
  operator HANDLE() const { return m_handle; }
};
#else
#define  PTP_CALLBACK_INSTANCE void*
#define  CALLBACK
#include <unistd.h>
typedef int native_file_handle;
#endif

namespace tpool
{
  typedef void (CALLBACK *callback_func)(PTP_CALLBACK_INSTANCE,void*);  
  struct task
  {
    callback_func m_func;
    void* m_arg;
  };

  enum aio_opcode
  {
    AIO_PREAD,
    AIO_PWRITE
  };
  const int MAX_AIO_USERDATA_LEN = 40;
  struct aiocb;
  typedef void (*aio_callback_func)(const aiocb* cb, int ret_len, int err);
  struct aiocb
  {
    native_file_handle m_fh;
    aio_opcode m_opcode;
    unsigned long long m_offset;
    void* m_buffer;
    unsigned int m_len;
    aio_callback_func m_callback;
    void* m_internal;
    char m_userdata[MAX_AIO_USERDATA_LEN];
  };

#ifdef _WIN32
  struct win_aio_cb : OVERLAPPED
  {
    aiocb m_aiocb;
  };
#endif

  class aio
  {
  public:
    virtual int submit_aio(const aiocb* cb) = 0;
    virtual int bind(native_file_handle& fd) = 0;
    virtual int unbind(const native_file_handle& fd) = 0;
    virtual ~aio(){};
  };

  class tpool;
  extern aio* create_simulated_aio(tpool* tp, int max_io);
#ifdef __linux__
  extern aio* create_linux_aio(tpool* tp, int max_io);
#endif
#ifdef _WIN32
  extern aio* create_win_aio(tpool* tp, int max_io);
#endif

  class tpool
  {
protected:
    aio *m_aio;
    virtual aio* create_native_aio(int max_io) = 0;
  public:
    virtual void submit(const task* tasks, int size) = 0;
    void submit(const task& t) { submit(&t,1); }
    int configure_aio(bool use_native_aio, int max_io)
    {
      if (use_native_aio)
        m_aio = create_native_aio(max_io);
      if (!m_aio)
        m_aio= create_simulated_aio(this,max_io);
      return !m_aio  ? -1 : 0;
    }
    int bind(native_file_handle &fd) 
    { 
      return m_aio->bind(fd);
    }
    void unbind(const native_file_handle& fd)
    {
      m_aio->unbind(fd);
    }
    int submit_io(const aiocb* cb)
    {
      return m_aio->submit_aio(cb);
    }
    virtual ~tpool(){ delete m_aio;}
  };
  extern tpool* create_tpool_generic();
#ifdef _WIN32
  extern tpool* create_tpool_win();
#endif
}
