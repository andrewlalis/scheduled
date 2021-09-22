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
    public void addJob(ScheduledJob job);

    /**
     * Adds a job to the scheduler, with the given schedule to define when it
     * should be run.
     * Params:
     *   job = The job to be added.
     *   schedule = The schedule defining when the job is run.
     */
    public final void addJob(Job job, JobSchedule schedule) {
        addJob(new ScheduledJob(job, schedule));
    }

    /** 
     * Adds a simple job that executes the given function according to the
     * given schedule.
     * Params:
     *   fn = A function to execute.
     *   schedule = The schedule defining when to execute the function.
     */
    public final void addJob(void function() fn, JobSchedule schedule) {
        addJob(new FunctionJob(fn), schedule);
    }

    /** 
     * Adds a job to the scheduler, whose schedule is defined by the given cron
     * expression string.
     * Params:
     *   job = The job to be added.
     *   cronExpressionString = A Cron expression string defining when to run the job.
     */
    public final void addCronJob(Job job, string cronExpressionString) {
        addJob(job, new CronSchedule(cronExpressionString));
    }

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

import core.thread;

/** 
 * A simple thread-based scheduler that sleeps until the next task, and runs it
 * using a task pool.
 */
public class ThreadedJobScheduler : Thread, JobScheduler {
    import std.parallelism;
    import std.container.binaryheap;
    import std.datetime.systime;
    import core.time;

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
     * The binary heap which serves as the job priority queue, where jobs are
     * ordered according to their next execution date.
     */
    private BinaryHeap!(ScheduledJob[]) jobPriorityQueue;

    /** 
     * Simple flag which is used to control this scheduler thread.
     */
    private shared bool running;

    /** 
     * Constructs a new threaded job scheduler.
     * Params:
     *   taskPool = The task pool to use when executing jobs.
     *   timeProvider = The component which provides current timestamps.
     */
    public this(TaskPool taskPool, CurrentTimeProvider timeProvider) {
        super(&this.run);
        this.taskPool = taskPool;
        this.timeProvider = timeProvider;
        this.jobPriorityQueue = BinaryHeap!(ScheduledJob[])([]);
    }

    /** 
     * Constructs a new threaded job scheduler with a default task pool and
     * time provider.
     */
    public this() {
        this(new TaskPool(1), new SysTimeProvider);
    }

    /** 
     * Adds a job to the scheduler.
     * Params:
     *   job = The job to be added. 
     */
    public void addJob(ScheduledJob job) {
        if (this.running) throw new Exception("Cannot add tasks while the scheduler is running.");
        this.jobPriorityQueue.insert(job);
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
        while (this.running && !this.jobPriorityQueue.empty) {
            ScheduledJob job = this.jobPriorityQueue.front;
            this.jobPriorityQueue.removeFront;
            SysTime now = this.timeProvider.now;
            auto nextExecutionTime = job.getSchedule.getNextExecutionTime(now);
            // If the job doesn't have a next execution, skip it, don't requeue it, and try again.
            if (nextExecutionTime.isNull) continue;
            Duration timeUntilJob = hnsecs(nextExecutionTime.get.stdTime - now.stdTime);
            
            // If the time until the next job is longer than our max sleep time, requeue the job and sleep as long as possible.
            if (MAX_SLEEP_TIME < timeUntilJob) {
                this.jobPriorityQueue.insert(job);
                this.sleep(MAX_SLEEP_TIME);
            } else {
                // The time until the next job is close enough that we can sleep directly to it.
                if (timeUntilJob > hnsecs(0)) {
                    this.sleep(timeUntilJob);
                }
                // Check if the scheduler was shutdown during its sleep, and exit if so.
                if (!this.running) break;
                // Queue up running the job, and process all other aspects of it.
                this.taskPool.put(task(&job.getJob.run));
                job.getSchedule.markExecuted(this.timeProvider.now);
                if (job.getSchedule.isRepeating) {
                    this.jobPriorityQueue.insert(job);
                }
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

    // Create a simple job which increments a variable by 1.
    class IncrementJob : Job {
        public uint x = 0;
        public void run() {
            x++;
        }
    }

    // Test case 1: Scheduler with a single job.

    JobScheduler scheduler = new ThreadedJobScheduler;
    auto inc1 = new IncrementJob;
    scheduler.addJob(inc1, new FixedIntervalSchedule(msecs(50)));
    scheduler.start();
    Thread.sleep(msecs(130));
    // We expect the job to be executed at t = 0, 50, and 100 ms.
    assert(inc1.x == 3, "Job did not execute the expected number of times.");
    scheduler.stop();

    // Test case 2: Scheduler with multiple jobs.

    JobScheduler scheduler2 = new ThreadedJobScheduler;
    auto incA = new IncrementJob;
    auto incB = new IncrementJob;
    scheduler2.addJob(incA, new FixedIntervalSchedule(msecs(50)));
    scheduler2.addJob(incB, new FixedIntervalSchedule(msecs(80)));
    scheduler2.start();
    Thread.sleep(msecs(180));
    // We expect job A to be executed at t = 0, 50, 100, and 150.
    assert(incA.x == 4, format("Job executed %d times instead of the expected %d.", incA.x, 4));
    // We expect job B to be executed at t = 0, 80, and 160.
    assert(incB.x == 3, format("Job executed %d times instead of the expected %d.", incB.x, 3));
    scheduler2.stop();
}
