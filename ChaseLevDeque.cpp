/**
 * \brief Chase-Lev "deque" data structure for concurrent work stealing
 *
 * This data structure is not actually a "deque" but is a stack on one
 * end, and a queue on the other end.  It was devised in:
 *
 * David Chase and Yossi Lev.  "Dynamic circular work-stealing deque".
 * Proceedings of the seventeenth annual ACM symposium on Parallelism
 * in algorithms and architectures, pages 21-28. July 18-20, 2005,
 * Las Vegas, Nevada, U.S.A.
 *
 * This code is heavily based on the version written in C11 portable
 * atomics by:
 *
 * Nhat Minh Le, Antoniu Pop, Albert Cohen and Francesco Zappa Nardelli.
 * "Correct and efficient work-stealing for weak memory models".
 * Proceedings of the 18th ACM SIGPLAN symposium on Principles and practice
 * of parallel programming, pages 69-80. February 23-27, 2013, Shenzhen, China.
 *
 * Following the original papers, the stack portion of the data structure
 * is called the "bottom" and the queue portion is called the "top".
 * Only one thread, called the owner, may push and pop elements at the
 * bottom.  All threads may "steal" elements from the top.  
 *
 * This asymmetricity lets us elide (relative) expensive synchronizations
 * for the owner thread.  Normally this data structure is used as the
 * local queue for one worker thread in a task scheduler, allowing other
 * worker threads to "steal" additional tasks to work on should they 
 * exhaust their own local tasks.
 */
class ChaseLevDeque 
{
  typedef void* ValueType;
  typedef unsigned int IndexType;

  static const IndexType MaxItems = ((1u<<31)-1);

  std::atomic<IndexType> topIndex;
  std::atomic<IndexType> bottomIndex;

  struct RingBuffer
  {
    ValueType* items;
    unsigned int lgSize_;

    public:
      static size_t roundUp( size_t n, size_t m )
      {
        return (n + (m-1))/m; 
      }

      static RingBuffer* allocate( unsigned int lgSize_in )
      {
        GELP_assert( lgSize_in < 32 );
        unsigned int numItems = (1u << lgSize_in);
        // FIXME overflow check
        size_t headerSize = roundUp(sizeof(RingBuffer), sizeof(ValueType));
        void* p = std::malloc(headerSize + numItems*sizeof(ValueType));
        RingBuffer* a = static_cast<RingBuffer*>(p);
        a->lgSize_ = lgSize_in;
        a->items = reinterpret_cast<ValueType*>(static_cast<unsigned char*>(p) + headerSize);
        return a;
      }

      static void deallocate( RingBuffer* a )
      {
        std::free(a);
      }

      unsigned int size() const { return 1u << lgSize_; }

      unsigned int lgSize() const { return lgSize_; }

      ValueType & operator[]( IndexType index )
      {
        return items[index & (size()-1)];
      }

      const ValueType & operator[]( IndexType index ) const
      {
        return items[index & (size()-1)];
      }
  };

  RingBuffer* ringBuffer;

  /**
   * \brief Take out the item at the bottom end, for the owner.
   *
   * Only the owner thread for this data structure may call this method.
   * Returns the default-constructed value if there is no item available.
   */
  ValueType pop()
  {
    RingBuffer<ValueType>* a = ringBuffer.load(std::memory_order_relaxed);

    #if _M_I386

    // On x86, lock xadd/add/sub/dec is slightly faster then the mfence
    // instruction that would have to be emitted afterwards, for correctness
    // in this algorithm, if we did not use atomic decrement here.
    IndexType b = --bottomIndex;

    #else

    // Reserve the item to be popped. 
    IndexType b = bottomIndex.load(std::memory_order_relaxed) - 1;
    bottomIndex.store(b, std::memory_order_relaxed);

    // See the comments in steal() for why this fence is here.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    #endif

    IndexType t = topIndex.load(std::memory_order_relaxed);
    ValueType x = ValueType();

    // Really testing if t <= b, but b, t are unsigned types
    // which wrap around zero on overflow.  b can go "smaller"
    // than t because b was just decremented above.
    IndexType s = b - t;
    if( s <= MaxItems )         // There is at least one item
    {
      x = (*a)[b];

      // t was strictly "smaller" than b, so we definitely own the
      // item now.
      if( t != b )
        return x;

      // If t == b, there is exactly one item.  It might already 
      // have been taken by a concurrent steal().  If so, we have
      // to increment b, at the end of this method, back to its
      // value before we decremented it earlier in this method, 
      // to fix things up and make the data structure consistent
      // again.  If we get the item, we have to get t and b both
      // incremented, to make things consistent.  Of course the
      // atomic increment itself to t is how we arbitrate the race.
      if( !topIndex.compare_exchange_strong(t, t+1, 
              std::memory_order_seq_cst, std::memory_order_relaxed) )
        x = ValueType();
    }

    bottomIndex.store(b+1, std::memory_order_relaxed);
  }

  RingBuffer* growBuffer()
  {
    RingBuffer* a_old = ringBuffer.load(std::memory_order_relaxed);
    RingBuffer* a_new = RingBuffer::allocate(a_old->lgSize()+1);
    for( unsigned int i = 0; i < a_old->size(); ++i )
      (*a_new)[i] = (*a_old)[i];
    ringBuffer.store(a_new, std::memory_order_relaxed);

    // Hazard pointer stuff goes here

    return a_new;
  }

  /**
   * \brief Put a new item at the bottom end, for the owner.
   *
   * Only the owner thread for this data structure may call this method.
   */
  void push( ValueType x )
  {
    IndexType b = bottomIndex.load(std::memory_order_relaxed);
    IndexType t = topIndex.load(std::memory_order_acquire);
    RingBuffer<ValueType>* a = ringBuffer.load(std::memory_order_relaxed);
    
    IndexType s = b - t;
    if( s >= a->size() )        // b-t cannot be negative
      a = growBuffer();
    
    (*a)[b] = x;
    bottomIndex.store(b+1, std::memory_order_release);
  }

  /**
   * \brief Attempt to steal an item from the top end.
   *
   * \param aborted  On return, set to true if stealing (temporarily) 
   *                 failed because this thread raced with another thread. 
   *                 Otherwise set to false.
   */
  ValueType steal( bool & aborted ) volatile
  {
    // std::memory_order_seq_cst here causes a concurrent take() to
    // receive the most up-to-date value of t if we race and this
    // load goes first.  This load synchronizes with the 
    // std::memory_order_seq_cst fence in take().  If, on the other
    // hand, this load goes second, then we guarantee b seen here is the
    // up-to-date decremented value from before the fence in take().
    IndexType t = topIndex.load(std::memory_order_seq_cst);
    IndexType b = bottomIndex.load(std::memory_order_relaxed);

    ValueType x = ValueType();

    // Really t < b, but b, t are unsigned types 
    // which wrap around zero on overflow. 
    IndexType s = b - t;
    if( s <= MaxItems && s != 0 )       // There is at least one item
    {
      RingBuffer* a = ringBuffer.load(std::memory_order_consume);

      // Attempt to take the item.
      x = (*a)[t];
      if (!topIndex.compare_exchange_strong(t, t+1, 
            std::memory_order_seq_cst, std::memory_order_relaxed) )
      {
        aborted = true;
        return ValueType();
      }
    }

    aborted = false;
    return x;
  }
}
