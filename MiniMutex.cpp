/**
 * \brief Mutex that shares kernel synchronization objects amongst
 *        all instances.
 *
 * Standard mutex implementations, e.g. boost::mutex and Windows'
 * critical sections, have to create kernel-level event objects
 * to wake up a thread after it goes sleeping trying to lock
 * an already locked mutex.  The kernel-level event objects are
 * not freed until the mutex object is destroyed.
 *
 * If an application uses thousands of mutexes in potentially 
 * long-lived objects, this behavior is not ideal.  But, an 
 * application will have a lot fewer threads, their number being
 * roughly proportional to the number of CPUs the host computer
 * has.  The number of concurrent waiters/users of a mutex can only
 * be at most the number of threads, so we ought to be able to
 * assign only at most one kernel-level synchronization object
 * to each active thread.
 *
 * This idea is not hard to implement assuming a very basic mutex
 * that supports only the minimum of operations: #lock, #try_lock
 * and #unlock.  We do not allow any timeouts on the locking,
 * the locking cannot be cancelled (from another thread), and a
 * mutex can be locked at most once by any thread (i.e. not a
 * recursive mutex).  Arguably, in correctly-designed concurrent
 * software where mutexes are to be held for only very short periods
 * of time, the extra features are not necessary.  
 *
 * An extra benefit of this minimalism is that this mutex class 
 * take up less space per instance than traditional "fat" mutexes.
 * Only spin-only mutexes can be smaller.
 *
 * The kernel-level synchronization objects are created lazily, 
 * as with standard mutexes, but are assigned instead to the 
 * thread using thread-local storage, not to the individual mutex.  
 *
 * The mutex holds a pointer to the dedicated thread-local storage
 * areas assigned to a locking or waiting thread.  If a new thread
 * comes in and sees there is at least one other thread working
 * with the mutex, it queues itself onto the waiting list.  
 * The waiting list is a singly-linked list stored in reverse order:
 * the head of the list is the last waiter, and the tail of the 
 * list is the current owner of the lock.  Naturally, an empty
 * waiting list means the mutex is unlocked.
 *
 * Queuing a thread is basically a compare-exchange operation.
 * By storing the waiting list in reverse order, we avoid the
 * usual "ABA" problem trying to insert a new entry
 * while trying to simultaneously link it together with the existing
 * entries.  If, after the compare-exchange operation, we find that 
 * the current thread represents the first entry in that linked
 * list, that means the current thread successfully took the lock.
 *
 * When it comes times to unlock, we traverse the waiting list, to
 * the entry before the last (which represents the current owner).
 * We may traverse the linked list with hardly any worry about 
 * concurrency.  Because we do not allow threads to cancel waiting
 * for a mutex, list entries cannot disappear in the middle of the
 * traversal.  Furthermore, new waiters only appear at the head
 * of the list, so we will never be traversing an outdated link.
 *
 * Once we reach the second last entry, we unlink the owner's last 
 * entry, and then pass the lock to the first waiting thread.  That 
 * is just signalling the kernel-level event object on that waiting 
 * thread.
 * 
 * Unlike some naive implementations of mutexes, only one thread is
 * woken up at a time, and the threads will be woken up in strict FIFO
 * order, following their queuing order in the waiting list.  So the 
 * scheduling is completely fair, and there is no "thundering herd" 
 * problem where multiple threads get woken up and then waste time 
 * contending with each other for the lock again.  
 * 
 * We do not speculatively spin, in a busy loop, when the lock is
 * being contended.  Doing so would introduce race conditions,
 * basically that the linked list of waiters cannot be safely traversed 
 * by any thread but the current owner of the lock.  These race conditions
 * can be avoided, but only by adding more complexity to the data
 * structures.  Then the benefit becomes questionable.  For if the mutex 
 * is known to be held, always, only for a short amount of time, 
 * on the order of a few CPU instructions, then a spin-only 
 * mutex should be used in the first place.
 */
class MiniMutex : private boost::noncopyable
{
  public:
    class Waiter;
  
  private:
    /**
     * \brief Stores the last "user" that touched this mutex.
     *
     * Once initialized, the mutex can be in one of three states.
     *
     * The "user" can be:
     *
     *    -# Null, meaning the mutex has not been locked.
     *    -# A MiniMutex::Waiter object whose MiniMutex::Waiter::waitingOn
     *       member is null.  That means the thread represented 
     *       by that object owns this mutex.
     *    -# A MiniMutex::Waiter object whose MiniMutex::Waiter::waitingOn
     *       member is not null, meaning it is waiting for another
     *       thread to give up the lock.  MiniMutex::lastWaiter points
     *       to the last thread that tried to contend for the lock
     *       and is blocked waiting. 
     */
    std::atomic<const Waiter*> lastWaiter;

    /// Increment statistic on the number of locks taken by this thread.
    void countLockSuccess( Waiter* thisWaiter )
    {
      thisWaiter->numLocks.store(
        thisWaiter->numLocks.load(std::memory_order_relaxed)+1,
        std::memory_order_relaxed);
    }

  public:

    /// Initializes the mutex to the unlocked state.
    MiniMutex()
      : lastWaiter(nullptr)
    {
    }

    /// Destroy the mutex.  Must be in unlocked state.
    ~MiniMutex()
    {
      GELP_assertR(!is_locked(),
                   "Mutex is being destroyed while being locked. ");
    }

    /// Whether the mutex has been locked by any thread.
    bool is_locked()
    {
      return this->lastWaiter.load(std::memory_order_relaxed) != nullptr;
    }

    /// Try to lock the mutex without blocking.  Returns true if successful.
    bool try_lock()
    {
      const Waiter* lastWaiter = nullptr;
      auto* thisWaiter = miniMutexWaiter.getPointerForThisThread();

      bool success = this->lastWaiter.compare_exchange_strong(
                      lastWaiter, thisWaiter, 
                      std::memory_order_acquire, std::memory_order_relaxed);

      if( success )
        countLockSuccess(thisWaiter);

      return success;
    }

    /// Lock the mutex, blocking the current thread if already locked. 
    void lock()
    {
      const Waiter* lastWaiter = nullptr;
      Waiter* thisWaiter = miniMutexWaiter.getPointerForThisThread();

      // Lock the mutex for this thread if there is no owner.
      if( this->lastWaiter.compare_exchange_strong(
            lastWaiter, thisWaiter, 
            std::memory_order_acquire, std::memory_order_relaxed) )
      {
        countLockSuccess(thisWaiter);
        return;
      }

      // Record statistics.
      thisWaiter->numContentions.store(
          thisWaiter->numContentions.load(std::memory_order_relaxed)+1,
          std::memory_order_relaxed);

      // Prepare kernel-level synchronization object.
      // Must be done before we post ourselves to the queue.
      thisWaiter->createEventObject();

      // Queue this thread as the last waiter for the mutex.
      //
      // std::memory_order_release here is to make sure the pointer to the
      // event object is propagated to the current owner of the mutex
      // when it unlocks.
      do {
        thisWaiter->waitingOn = lastWaiter;
      } while( !this->lastWaiter.compare_exchange_weak(
                  lastWaiter, thisWaiter, 
                  std::memory_order_release, std::memory_order_relaxed) );

      // If lastWaiter == nullptr, the mutex may have got released before 
      // we managed to set up our event object and queued this thread to 
      // wait.  Fortunately, the compare_exchange_weak operation we just 
      // did has already locked the mutex without allowing further races.
      //
      // Otherwise, we go to sleep, and have to wait for unlock() from the 
      // last owner to wake up.
      if( lastWaiter != nullptr )
      {
        thisWaiter->wait();
        thisWaiter->waitingOn = nullptr;
      }

      // This thread has now locked the mutex.
      // Make sure all writes to data protected by the mutex are propagated
      // to this thread.  
      countLockSuccess(thisWaiter);
      std::atomic_thread_fence(std::memory_order_acquire);
    }

    /// Unlock the mutex.  This thread must have already locked it.
    void unlock()
    {
      Waiter* thisWaiter = miniMutexWaiter.getPointerForThisThread();
      const Waiter* lastWaiter = this->lastWaiter.load(std::memory_order_relaxed);

      GELP_assertR(lastWaiter != nullptr &&
                   thisWaiter->waitingOn == nullptr,
                   "Attempt to unlock a mutex that was not locked by the current thread. ");

      // If we see that there are no threads waiting for this mutex, 
      // attempt to just unlock it.  If we race with another thread
      // trying to queue itself for waiting, then we do not exit early.
      //
      // std::memory_order_release ensures the data protected by this mutex
      // is published for a later thread that locks the mutex.
      if( lastWaiter == thisWaiter &&
          this->lastWaiter.compare_exchange_strong(
              lastWaiter, nullptr, 
              std::memory_order_release, std::memory_order_relaxed) )
        return;

      // Ensure all writes to Waiter::waitingOn from other threads are 
      // visible to this thread.  At the same time ensure the data
      // protected by this mutex is published to the thread that will
      // soon receive the lock.
      std::atomic_thread_fence(std::memory_order_acq_rel);

      // Traverse to the entry in the waiting list just before thisWaiter.
      const Waiter* nextWaiter = lastWaiter;
      while( true )
      {
        GELP_assertR(nextWaiter != thisWaiter,
                     "The current thread locked the same non-recursive mutex twice. ");
        const Waiter* prevWaiter = nextWaiter->waitingOn;
        if( prevWaiter == thisWaiter )
          break;
        GELP_assert(prevWaiter != nullptr,
                    "Attempt to unlock a mutex that was not locked by the current thread. ");
        nextWaiter = prevWaiter;
      }

      // Pass the lock to the first waiting thread.
      nextWaiter->signal();
    }
};


