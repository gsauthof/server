#include "tpool_structs.h"
#include <algorithm>
#include <assert.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <limits.h>
#include <mutex>
#include <queue>
#include <stack>
#include <thread>
#include <vector>
#include <tpool.h>
#include <assert.h>

namespace tpool
{

enum worker_wake_reason
{
  WAKE_REASON_NONE,
  WAKE_REASON_TASK,
  WAKE_REASON_DIE,
  WAKE_REASON_SHUTDOWN
};

struct worker_data
{
  std::condition_variable m_cv;
  worker_wake_reason m_wake_reason;
  task m_task;
  worker_data() : m_cv(), m_wake_reason(WAKE_REASON_NONE), m_task{0, 0} {}
};

class tpool_generic : public tpool
{
  circular_queue<task> m_tasks;
  std::vector<worker_data *> m_standby_threads;
  std::mutex m_mtx;
  std::chrono::milliseconds m_thread_timeout;
  std::chrono::milliseconds m_timer_interval;
  std::condition_variable m_cv_no_active_threads;
  std::condition_variable m_cv_no_threads;
  std::condition_variable m_cv_queue_not_full;
  std::condition_variable m_cv_shutdown;
  std::thread m_timer_thread;
  int m_threads;
  int m_active_threads;
  int m_tasks_dequeued;
  int m_wakeups;
  int m_spurious_wakeups;
  int m_concurrency;
  bool m_in_shutdown;
  bool m_stopped;
  int m_min_threads;
  int m_max_threads;

  void worker_main();
  void worker_end();
  void timer_main();
  bool add_thread();
  bool wake(worker_wake_reason reason, const task *t= nullptr);
  void wake_or_create_thread();
  bool get_task(worker_data *thread_var, task *t);
  bool wait_for_tasks(std::unique_lock<std::mutex> &lk,
                      worker_data *thread_var);
  void timer_start();
  void timer_stop();
public:
  tpool_generic();
  ~tpool_generic() { shutdown(); }
  void submit(const task *tasks, int size);
  void set_min_threads(int);
  void set_max_threads(int);
  void shutdown();

  // Inherited via tpool
  virtual aio *create_native_aio(int max_io) override
  {
#ifdef _WIN32
    return create_win_aio(this, max_io);
#elif defined(__linux__)
    return create_linux_aio(this,max_io);
#else
    return nullptr;
#endif
  }
};

bool tpool_generic::wait_for_tasks(std::unique_lock<std::mutex> &lk,
                                   worker_data *thread_data)
{
  assert(m_tasks.empty());
  assert(!m_in_shutdown);

  thread_data->m_wake_reason= WAKE_REASON_NONE;
  m_standby_threads.push_back(thread_data);
  m_active_threads--;

  for (;;)
  {
    thread_data->m_cv.wait_for(lk, m_thread_timeout);
    if (thread_data->m_wake_reason != WAKE_REASON_NONE)
    {
      return true;
    }

    if (m_threads <= m_min_threads)
    {
      continue;
    }

    /*
      Woke up due to timeout, remove this thread's  from the standby list. In
      all other cases where it is signaled it is removed by the signaling
      thread.
    */
    auto it= std::find(m_standby_threads.begin(), m_standby_threads.end(),
                       thread_data);
    m_standby_threads.erase(it);
    m_active_threads++;
    return false;
  }

  return !m_tasks.empty() && m_threads >= m_min_threads;
}

bool tpool_generic::get_task(worker_data *thread_var, task *t)
{
  std::unique_lock<std::mutex> lk(m_mtx);
  if (m_tasks.empty())
  {
    if (m_in_shutdown)
      return false;

    if (!wait_for_tasks(lk, thread_var))
      return false;
    if (thread_var->m_wake_reason == WAKE_REASON_TASK)
    {
      *t= thread_var->m_task;
      thread_var->m_task.m_func= 0;
      return true;
    }
    if (thread_var->m_wake_reason == WAKE_REASON_DIE)
      return false;

    if (m_tasks.empty())
      return false;
  }

  bool was_full= m_tasks.full();
  *t= m_tasks.front();
  m_tasks.pop();
  m_tasks_dequeued++;
  if (was_full)
  {
    m_cv_queue_not_full.notify_all();
  }
  return true;
}

void tpool_generic::worker_end()
{
  std::lock_guard<std::mutex> lk(m_mtx);
  m_threads--;
  m_active_threads--;

  if (!m_threads && m_in_shutdown)
  {
    m_cv_no_threads.notify_all();
  }
}

void tpool_generic::worker_main()
{
  worker_data thread_var;
  task task;

  while (get_task(&thread_var, &task))
  {
    task.m_func(0, task.m_arg);
  }

  worker_end();
}

void tpool_generic::timer_main()
{
  int last_tasks_dequeued= 0;
  int last_threads= 0;
  for (;;)
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    m_cv_shutdown.wait_for(lk, m_timer_interval);

    if (m_in_shutdown && m_tasks.empty())
      return;
    if (m_tasks.empty())
      continue;

    if (m_active_threads < m_concurrency)
    {
      wake_or_create_thread();
      continue;
    }

    if (!m_tasks.empty() && last_tasks_dequeued == m_tasks_dequeued &&
        last_threads <= m_threads && m_active_threads == m_threads)
    {
      // no progress made since last iteration. create new
      // thread
      add_thread();
    }
    lk.unlock();
    last_tasks_dequeued= m_tasks_dequeued;
    last_threads= m_threads;
  }
}

bool tpool_generic::add_thread()
{
  if (m_threads >= m_max_threads)
    return false;
  m_threads++;
  m_active_threads++;
  std::thread thread(&tpool_generic::worker_main, this);
  thread.detach();
  return true;
}

bool tpool_generic::wake(worker_wake_reason reason, const task *t)
{
  assert(reason != WAKE_REASON_NONE);

  if (m_standby_threads.empty())
    return false;
  auto var= m_standby_threads.back();
  m_standby_threads.pop_back();
  m_active_threads++;
  assert(var->m_wake_reason == WAKE_REASON_NONE);
  var->m_wake_reason= reason;
  var->m_cv.notify_one();
  if (t)
  {
    var->m_task= *t;
  }
  m_wakeups++;
  return true;
}

void tpool_generic::timer_start()
{
  m_timer_thread = std::thread(&tpool_generic::timer_main, this);
}

void tpool_generic::timer_stop()
{
  assert(m_in_shutdown || m_max_threads == m_min_threads);
  m_cv_shutdown.notify_one();
  m_timer_thread.join();
}

tpool_generic::tpool_generic()
    : m_tasks(10000), m_standby_threads(), m_mtx(),
      m_thread_timeout(std::chrono::milliseconds(60000)),
      m_timer_interval(std::chrono::milliseconds(10)), m_cv_no_threads(),
      m_cv_shutdown(), m_threads(), m_active_threads(), m_tasks_dequeued(),
      m_wakeups(), m_spurious_wakeups(),
      m_concurrency(std::thread::hardware_concurrency()), m_in_shutdown(),
      m_stopped(), m_min_threads(0), m_max_threads(INT_MAX)
{
  timer_start();
}

void tpool_generic::set_min_threads(int n)
{
  std::unique_lock<std::mutex> lk(m_mtx);
  if (n == m_min_threads)
    return;


  m_min_threads= n;
  if (m_max_threads < n)
    m_max_threads = n;

  for (auto i= m_threads; i < m_min_threads; i++)
    add_thread();
}

void tpool_generic::set_max_threads(int n)
{
  std::unique_lock<std::mutex> lk(m_mtx);
  if (n == m_max_threads)
    return;
  m_max_threads= n;
  if (n < m_min_threads)
    m_min_threads = n;

  for (auto i= m_max_threads; i < m_threads; i++)
    wake(WAKE_REASON_DIE);
}

void tpool_generic::wake_or_create_thread()
{
  assert(!m_tasks.empty());
  if (!m_standby_threads.empty())
  {
    task &t= m_tasks.front();
    m_tasks.pop();
    wake(WAKE_REASON_TASK, &t);
  }
  else
  {
    add_thread();
  }
}

void tpool_generic::submit(const task *tasks, int size)
{
  std::unique_lock<std::mutex> lk(m_mtx);

  for (auto i= 0; i < size; i++)
  {
    while (m_tasks.full())
    {
      m_cv_queue_not_full.wait(lk);
    }
    if (m_in_shutdown)
      return;
    m_tasks.push(tasks[i]);
  }

  // int n = std::min(m_concurrency - m_active_threads, (int)m_tasks.size());

  bool do_wake= m_active_threads < m_concurrency;
  if (do_wake)
    wake_or_create_thread();
}

void tpool_generic::shutdown()
{
  std::unique_lock<std::mutex> lk(m_mtx);
  if (m_stopped)
    return;
  m_in_shutdown= true;

  /* Wake up idle threads. */
  while (wake(WAKE_REASON_SHUTDOWN))
  {
  }

  while (m_threads)
  {
    m_cv_no_threads.wait(lk);
  }

  lk.unlock();

  timer_stop();

  m_cv_queue_not_full.notify_all();
  m_stopped= true;
}

tpool *create_tpool_generic() { return new tpool_generic(); }
} // namespace tpool
