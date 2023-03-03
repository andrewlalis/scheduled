module scheduled.taskpool_scheduler;

import scheduled.scheduler;
import scheduled.schedule;
import scheduled.job;
import core.thread;
import slf4d;

/** 
 * A simple thread-based scheduler that sleeps until the next task, and runs it
 * using a task pool. Allows for adding and removing jobs only when not
 * running.
 */
public class TaskPoolScheduler : Thread, MutableJobScheduler {
    import std.parallelism;
    import std.datetime.systime;
    import std.algorithm.mutation;
    import std.typecons : Nullable;
    import std.range : empty;
    import core.time;
    import core.atomic;
    import core.sync.semaphore;
    import core.sync.mutex;

    /** 
     * The maximum amount of time that this scheduler may sleep for. This is
     * mainly used as a sanity check against clock deviations or other
     * inconsistencies in timings.
     */
    private static immutable Duration MAX_SLEEP_TIME = seconds(60);

    /** 
     * The component which provides the current timestamp.
     */
    private CurrentTimeProvider timeProvider;

    /** 
     * The task pool that is used to execute jobs.
     */
    private TaskPool taskPool;

    /** 
     * The set of scheduled jobs that this scheduler tracks. This list will be
     * maintained in a sorted order, whereby the first job is the one which
     * needs to be executed next.
     */
    private ScheduledJob[] jobs;

    /** 
     * A mutex for controlling access to the list of jobs this scheduler has.
     */
    private shared Mutex jobsMutex;

    /** 
     * Simple flag which is used to control this scheduler thread.
     */
    private shared bool running;

    /** 
     * Flag that's set to true when this scheduler is empty and waiting for
     * new jobs to be added.
     */
    private shared bool waiting = false;

    /** 
     * A semaphore that is used to notify a waiting scheduler that a job has
     * been added to its list, and that it can resume normal operations.
     */
    private Semaphore emptyJobsSemaphore;

    /** 
     * Simple auto-incrementing id that is used to issue ids to new scheduled
     * jobs. Due to the nature of the world, we can safely assume that we will
     * never run out of ids.
     */
    private shared ulong nextId = 1;

    /** 
     * Constructs a new threaded job scheduler.
     * Params:
     *   taskPool = The task pool to use when executing jobs.
     *   timeProvider = The component which provides current timestamps.
     */
    public this(TaskPool taskPool, CurrentTimeProvider timeProvider) {
        super(&this.run);
        this.jobsMutex = new shared Mutex();
        this.emptyJobsSemaphore = new Semaphore();
        this.taskPool = taskPool;
        this.timeProvider = timeProvider;
    }

    /** 
     * Constructs a new threaded job scheduler with a default task pool and
     * time provider.
     */
    public this() {
        this(new TaskPool(1), new SysTimeProvider());
    }

    /** 
     * Adds a job to the scheduler. For this scheduler, jobs are added to the
     * list in-order, such that the first job is the one whose next execution
     * time is the closest.
     * Params:
     *   job = The job to be added. 
     */
    public void addScheduledJob(ScheduledJob job) {
        synchronized(this.jobsMutex) {
            this.jobs ~= job;
            this.sortJobs();
        }
        if (this.waiting) {
            this.emptyJobsSemaphore.notify();
        }
        auto log = getLogger();
        log.debugF!"Added scheduled job %s with id %d."(job.job, job.id);
    }

    /** 
     * Removes a job from the scheduler.
     * Params:
     *   job = The job to be removed.
     * Returns: True if the job was removed, or false otherwise.
     */
    public bool removeScheduledJob(ScheduledJob job) {
        size_t idx = -1;
        synchronized(this.jobsMutex) {
            foreach (i, j; this.jobs) {
                if (j.id == job.id) {
                    idx = i;
                    break;
                }
            }
            if (idx != -1) {
                this.jobs = this.jobs.remove(idx);
                getLogger().debugF!"Removed job %s (id %d)."(job.job, job.id);
                return true;
            }
            return false;
        }
    }

    /** 
     * Sorts the internal list of jobs according to the next execution time.
     * Jobs that execute sooner appear earlier in the list. Only call this
     * in synchronized code.
     */
    private void sortJobs() {
        import std.algorithm : sort;

        int[] data = [1, 3, 4, -1, 2, 6, -4];
        data.sort!((a, b) {
            return a > b;
        });

        auto now = this.timeProvider.now();
        jobs.sort!((a, b) {
            if (a.id == b.id) return false;
            auto t1 = a.schedule.getNextExecutionTime(now);
            auto t2 = b.schedule.getNextExecutionTime(now);
            if (t1.isNull && t2.isNull) return false;
            if (!t1.isNull && t2.isNull) return true;
            if (t1.isNull) return false;
            return t1.get().opCmp(t2.get()) < 0;
        });
    }

    /** 
     * Removes all jobs from the scheduler.
     */
    public void removeAllScheduledJobs() {
        synchronized(this.jobsMutex) {
            this.jobs = [];
        }
    }

    /** 
     * Gets the number of jobs that this scheduler has.
     * Returns: The number of jobs currently scheduled.
     */
    public ulong jobCount() {
        synchronized(this.jobsMutex) {
            return this.jobs.length;
        }
    }

    protected CurrentTimeProvider getTimeProvider() {
        return this.timeProvider;
    }

    /** 
     * Gets the next available id to assign to a scheduled job. This must be
     * unique among all jobs that have been added to the scheduler but not yet
     * removed.
     * Returns: The next id to use when adding a scheduled job.
     */
    protected ulong getNextScheduledJobId() {
        ulong id = atomicLoad(this.nextId);
        atomicOp!"+="(this.nextId, 1);
        return id;
    }

    /**
     * Starts the scheduler. Once started, there is no guarantee that all
     * scheduler implementations will allow adding new jobs while running.
     */
    public void start() {
        super.start();
    }

    /** 
     * Runs the scheduler. This works by popping the next scheduled task from
     * the priority queue (since scheduled tasks are ordered by their next
     * execution date) and sleeping until we reach that task's execution date.
     */
    private void run() {
        auto log = getLogger();
        this.running = true;
        while (this.running) {
            if (!this.jobs.empty) {
                this.jobsMutex.lock_nothrow();

                ScheduledJob nextJob = this.jobs[0];
                SysTime now = this.timeProvider.now();
                Nullable!SysTime nextExecutionTime = nextJob.schedule.getNextExecutionTime(now);
                // If the job doesn't have a next execution, simply remove it.
                if (nextExecutionTime.isNull) {
                    log.debugF!"Removing job %s (id %d) because it doesn't have a next execution time."
                        (nextJob.job, nextJob.id);
                    this.jobs = this.jobs.remove(0);
                    this.jobsMutex.unlock_nothrow();
                } else {
                    Duration timeUntilJob = hnsecs(nextExecutionTime.get.stdTime - now.stdTime);
                    // If it's time to execute this job, then we run it now!
                    if (timeUntilJob.isNegative) {
                        log.debugF!"Running job %s (id %d)."(nextJob.job, nextJob.id);
                        this.taskPool.put(task(&nextJob.job.run));
                        nextJob.schedule.markExecuted(now);
                        this.jobs = this.jobs.remove(0);
                        if (nextJob.schedule.isRepeating) {
                            log.debugF!"Requeued job %s (id %d) because its schedule is repeating."
                                (nextJob.job, nextJob.id);
                            this.jobs ~= nextJob;
                            this.sortJobs();
                        }
                        this.jobsMutex.unlock_nothrow();
                    } else {
                        this.jobsMutex.unlock_nothrow();
                        // Otherwise, we sleep until the next job is ready, then try again.
                        Duration timeToSleep = MAX_SLEEP_TIME < timeUntilJob ? MAX_SLEEP_TIME : timeUntilJob;
                        log.debugF!"Sleeping for %d ms."(timeToSleep.total!"msecs");
                        this.sleep(timeToSleep);
                    }
                }
            } else {
                log.debug_("No jobs in queue. Waiting until a job is added.");
                this.waiting = true;
                this.emptyJobsSemaphore.wait();
            }
        }
    }

    /**
     * Stops the scheduler.
     * Params:
     *   force = Whether to forcibly shutdown, cancelling any current jobs.
     */
    public void stop(bool force = false) {
        this.running = false;
        if (waiting) emptyJobsSemaphore.notify();
        if (force) {
            this.taskPool.stop();
        } else {
            this.taskPool.finish(true);
        }
    }
}

/**
 * Tests the functionality of the threaded job scheduler, by running it for
 * some different simple jobs to ensure the jobs are executed properly.
 */
unittest {
    import core.thread;
    import core.atomic;
    import std.format;
    import std.experimental.logger;
    import scheduled.schedules.fixed_interval;
    import std.stdio;
    import slf4d;
    import slf4d.default_provider;

    auto prov = new shared DefaultProvider();
    prov.getLoggerFactory.setModuleLevel("scheduled.threadpool_scheduler", Levels.DEBUG);
    configureLoggingProvider(prov);

    // Run standard Scheduler test suite:
    testScheduler(() => new TaskPoolScheduler());

    // Create a simple job which increments a variable by 1.
    class IncrementJob : Job {
        public uint x = 0;
        public string id;
        public this(string id) {
            this.id = id;
        }

        public void run() {
            x++;
        }
    }

    void assertJobStatus(IncrementJob j, uint expected) {
        assert(j.x == expected, format("Job %s executed %d times instead of the expected %d.", j.id, j.x, expected));
    }

    // Test case 1: Scheduler with a single job.

    TaskPoolScheduler scheduler = new TaskPoolScheduler();
    auto inc1 = new IncrementJob("1");
    scheduler.addJob(inc1, new FixedIntervalSchedule(msecs(50)));
    scheduler.start();
    Thread.sleep(msecs(130));
    // We expect the job to be executed at t = 0, 50, and 100 ms.
    assert(inc1.x == 3, "Job did not execute the expected number of times.");
    scheduler.stop();

    // Test case 2: Scheduler with multiple jobs.
    TaskPoolScheduler scheduler2 = new TaskPoolScheduler();
    auto incA = new IncrementJob("A");
    auto incB = new IncrementJob("B");
    ScheduledJob sjA = scheduler2.addJob(incA, new FixedIntervalSchedule(msecs(50)));
    ScheduledJob sjB = scheduler2.addJob(incB, new FixedIntervalSchedule(msecs(80)));
    assert(scheduler2.jobCount == 2);
    scheduler2.start();
    Thread.sleep(msecs(180));
    // We expect job A to be executed at t = 0, 50, 100, and 150.
    assertJobStatus(incA, 4);
    // We expect job B to be executed at t = 0, 80, and 160.
    assertJobStatus(incB, 3);
    // Try and remove a job.
    assert(scheduler2.removeScheduledJob(sjA));
    assert(scheduler2.jobCount == 1);
    assert(!scheduler2.removeScheduledJob(sjA));
    Thread.sleep(msecs(170));
    // We expect job B to be executed at t = 0, 80.
    assertJobStatus(incB, 5);
    // We expect job A to not be executed since its scheduled job was removed.
    assertJobStatus(incA, 4);
    
    // Remove all jobs, wait a bit, and add one back.
    assert(scheduler2.removeScheduledJob(sjB));
    assert(scheduler2.jobCount == 0);
    Thread.sleep(msecs(100));
    auto incC = new IncrementJob("C");
    ScheduledJob sjC = scheduler2.addJob(incC, new FixedIntervalSchedule(msecs(30)));
    assert(scheduler2.jobCount == 1);
    Thread.sleep(msecs(100));
    // We expect job C to be executed at t = 0, 30, 60, 90.
    assertJobStatus(incC, 4);
    scheduler2.stop(false);
}
