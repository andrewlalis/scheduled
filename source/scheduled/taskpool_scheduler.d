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
        this.taskPool.isDaemon = true;
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

unittest {
    // Run standard Scheduler test suite:
    testScheduler(() => new TaskPoolScheduler());
}
