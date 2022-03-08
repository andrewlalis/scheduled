/**
 * Defines the scheduler interface, and a simple thread-based implementation
 * of the scheduler.
 */
module scheduled.scheduler;

import scheduled.job;
import scheduled.schedule;
import scheduled.schedules.cron_schedule;

/** 
 * A scheduler is the core component of the library; you add jobs to the job
 * scheduler, and then it will execute these according to the job's schedule.
 */
public interface JobScheduler {
    /** 
     * Adds a job to the scheduler.
     * Params:
     *   job = The job to be added. 
     */
    protected void addScheduledJob(ScheduledJob job);

    /**
     * Adds a job to the scheduler, with the given schedule to define when it
     * should be run.
     * Params:
     *   job = The job to be added.
     *   schedule = The schedule defining when the job is run.
     * Returns: The scheduled job.
     */
    public final ScheduledJob addJob(Job job, JobSchedule schedule) {
        auto sj = new ScheduledJob(job, schedule, getNextScheduledJobId());
        addScheduledJob(sj);
        return sj;
    }

    /** 
     * Adds a simple job that executes the given function according to the
     * given schedule.
     * Params:
     *   fn = A function to execute.
     *   schedule = The schedule defining when to execute the function.
     * Returns: The scheduled job.
     */
    public final ScheduledJob addJob(void function() fn, JobSchedule schedule) {
        return addJob(new FunctionJob(fn), schedule);
    }

    /** 
     * Adds a job to the scheduler, whose schedule is defined by the given cron
     * expression string.
     * Params:
     *   job = The job to be added.
     *   cronExpressionString = A Cron expression string defining when to run the job.
     * Returns: The scheduled job.
     */
    public final ScheduledJob addCronJob(Job job, string cronExpressionString) {
        return addJob(job, new CronSchedule(cronExpressionString));
    }

    /** 
     * Gets the next available id to assign to a scheduled job. This must be
     * unique among all jobs that have been added to the scheduler but not yet
     * removed.
     * Returns: The next id to use when adding a scheduled job.
     */
    protected ulong getNextScheduledJobId();

    /**
     * Starts the scheduler. Once started, there is no guarantee that all
     * scheduler implementations will allow adding new jobs while running.
     */
    public void start();

    /**
     * Stops the scheduler.
     * Params:
     *   force = Whether to forcibly shutdown, cancelling any current jobs.
     */
    public void stop(bool force);

    /** 
     * Stops the scheduler, and waits for any currently-executing jobs to
     * finish. Functionally equivalent to calling stop(false).
     */
    public final void stop() {
        stop(false);
    }
}

/** 
 * A job scheduler which offers additional functionality for modifying the set
 * of scheduled jobs after they're submitted.
 */
public interface MutableJobScheduler : JobScheduler {
    /** 
     * Removes a job from the scheduler.
     * Params:
     *   job = The job to remove.
     * Returns: True if the job was removed, or false otherwise.
     */
    public bool removeScheduledJob(ScheduledJob job);
}

import core.thread;

/** 
 * A simple thread-based scheduler that sleeps until the next task, and runs it
 * using a task pool. Allows for adding and removing jobs only when not
 * running.
 */
public class ThreadedJobScheduler : Thread, MutableJobScheduler {
    import std.parallelism;
    import std.datetime.systime;
    import std.algorithm.mutation;
    import std.algorithm.sorting : sort;
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
     * new jobs to be added. This indicates.
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
        this(new TaskPool(1), new SysTimeProvider);
    }

    /** 
     * Adds a job to the scheduler. For this scheduler, jobs are added to the
     * list in-order, such that the first job is the one whose next execution
     * time is the closest.
     * Params:
     *   job = The job to be added. 
     */
    public void addScheduledJob(ScheduledJob job) {
        this.jobsMutex.lock_nothrow();
        this.jobs ~= job;
        this.jobs.sort!("a > b");
        this.jobsMutex.unlock_nothrow();
        if (this.waiting) {
            this.emptyJobsSemaphore.notify();
        }
    }

    /** 
     * Removes a job from the scheduler.
     * Params:
     *   job = The job to be removed.
     * Returns: True if the job was removed, or false otherwise.
     */
    public bool removeScheduledJob(ScheduledJob job) {
        size_t idx = -1;
        this.jobsMutex.lock_nothrow();
        foreach (i, j; this.jobs) {
            if (j.getId() == job.getId()) {
                idx = i;
                break;
            }
        }
        if (idx != -1) {
            this.jobs = this.jobs.remove(idx);
            this.jobsMutex.unlock_nothrow();
            return true;
        }
        this.jobsMutex.unlock_nothrow();
        return false;
    }

    /** 
     * Removes all jobs from the scheduler.
     */
    public void removeAllScheduledJobs() {
        this.jobsMutex.lock_nothrow();
        this.jobs = [];
        this.jobsMutex.unlock_nothrow();
    }

    /** 
     * Gets the number of jobs that this scheduler has.
     * Returns: The number of jobs currently scheduled.
     */
    public ulong jobCount() {
        return this.jobs.length;
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
        this.running = true;
        while (this.running) {
            if (!this.jobs.empty) {
                this.jobsMutex.lock_nothrow();
                ScheduledJob nextJob = this.jobs[0];
                SysTime now = this.timeProvider.now();
                Nullable!SysTime nextExecutionTime = nextJob.getSchedule().getNextExecutionTime(now);
                // If the job doesn't have a next execution, simply remove it.
                if (nextExecutionTime.isNull) {
                    this.jobs = this.jobs.remove(0);
                    this.jobsMutex.unlock_nothrow();
                } else {
                    Duration timeUntilJob = hnsecs(nextExecutionTime.get.stdTime - now.stdTime);
                    // If it's time to execute this job, then we run it now!
                    if (timeUntilJob.isNegative) {
                        this.taskPool.put(task(&nextJob.getJob.run));
                        nextJob.getSchedule.markExecuted(now);
                        this.jobs = this.jobs.remove(0);
                        if (nextJob.getSchedule.isRepeating) {
                            this.jobs ~= nextJob;
                            this.jobs.sort!("a > b");
                        }
                        this.jobsMutex.unlock_nothrow();
                    } else {
                        this.jobsMutex.unlock_nothrow();
                        // Otherwise, we sleep until the next job is ready, then try again.
                        if (MAX_SLEEP_TIME < timeUntilJob) {
                            this.sleep(MAX_SLEEP_TIME);
                        } else {
                            this.sleep(timeUntilJob);
                        }
                    }
                }
            } else {
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
    public void stop(bool force) {
        this.running = false;
        this.taskPool.stop();
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

    // Create a simple job which increments a variable by 1.
    class IncrementJob : Job {
        public uint x = 0;
        public string id;
        public this(string id) {
            this.id = id;
        }

        public void run() {
            x++;
            import std.stdio;
            writefln!"[%s] Incrementing x to %d"(id, x);
        }
    }

    void assertJobStatus(IncrementJob j, uint expected) {
        assert(j.x == expected, format("Job %s executed %d times instead of the expected %d.", j.id, j.x, expected));
    }

    // Test case 1: Scheduler with a single job.

    JobScheduler scheduler = new ThreadedJobScheduler;
    auto inc1 = new IncrementJob("1");
    scheduler.addJob(inc1, new FixedIntervalSchedule(msecs(50)));
    scheduler.start();
    Thread.sleep(msecs(130));
    // We expect the job to be executed at t = 0, 50, and 100 ms.
    assert(inc1.x == 3, "Job did not execute the expected number of times.");
    scheduler.stop();

    // Test case 2: Scheduler with multiple jobs.
    writeln("Scheduler 1 complete");

    ThreadedJobScheduler scheduler2 = new ThreadedJobScheduler;
    auto incA = new IncrementJob("A");
    auto incB = new IncrementJob("B");
    ScheduledJob sjA = scheduler2.addJob(incA, new FixedIntervalSchedule(msecs(50)));
    ScheduledJob sjB = scheduler2.addJob(incB, new FixedIntervalSchedule(msecs(80)));
    assert(scheduler2.jobCount == 2);
    scheduler2.start();
    writeln("Starting scheduler 2");
    Thread.sleep(msecs(180));
    // We expect job A to be executed at t = 0, 50, 100, and 150.
    assertJobStatus(incA, 4);
    // We expect job B to be executed at t = 0, 80, and 160.
    assertJobStatus(incB, 3);
    // Try and remove a job.
    writeln("Removing scheduled job A");
    assert(scheduler2.removeScheduledJob(sjA));
    assert(scheduler2.jobCount == 1);
    assert(!scheduler2.removeScheduledJob(sjA));
    Thread.sleep(msecs(170));
    // We expect job B to be executed at t = 0, 80.
    assertJobStatus(incB, 5);
    // We expect job A to not be executed since its scheduled job was removed.
    assertJobStatus(incA, 4);
    
    // Remove all jobs, wait a bit, and add one back.
    writeln("Removing scheduled job B and waiting a while.");
    assert(scheduler2.removeScheduledJob(sjB));
    assert(scheduler2.jobCount == 0);
    Thread.sleep(msecs(100));
    writeln("Adding scheduled job C");
    auto incC = new IncrementJob("C");
    ScheduledJob sjC = scheduler2.addJob(incC, new FixedIntervalSchedule(msecs(30)));
    assert(scheduler2.jobCount == 1);
    Thread.sleep(msecs(100));
    // We expect job C to be executed at t = 0, 30, 60, 90.
    assertJobStatus(incC, 4);
    scheduler2.stop(false);
}
