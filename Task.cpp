class Task
{
  public:
    virtual Task* runLocally( Worker* worker ) = 0;
    virtual void abandon( Worker* worker ) = 0;
};

class Worker : private boost::noncopyable
{
  public:
    void setToTerminate();

    Worker();
    ~Worker();

  private:
    friend class Scheduler;

    Scheduler & scheduler;

    void startThread();
    void abandonAllLocalTasks();
    void threadLoop();
};

void
Worker::threadLoop()
{
  // Loop indefinitely, until thread is shut down
  for(;;)
  {
    // block waiting for signal

    Task* nextTask = nullptr;

    for(;;)
    {
        Task* currentTask;

        while( currentTask = controlQueue->tryGet() )
        {
          if( nextTask )
          {
            this->pushLocalTask(nextTask);
            nextTask = nullptr;
          }

          do {
            currentTask = currentTask->runLocally(this);
          } while( currentTask );

          if( this->isTerminating() )
          {
            this->abandonAllLocalTasks();
            return;
          }
        }

        if( currentTask = nextTask ) ;
        else if( currentTask = workerStack->tryGet() ) ;
        else if( currentTask = workerQueue->tryGet() ) ;
        else if( currentTask = globalQueue->tryGet() ) ;
        else break;

        nextTask = currentTask->runLocally(this);
    }
  }
}
