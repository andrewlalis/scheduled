/**
 * Defines the scheduler interface, and a simple thread-based implementation
 * of the scheduler.
 */
module scheduled.scheduler;

import scheduled.job;
import scheduled.schedule;
import scheduled.schedules.cron_schedule;

/** 
 * A pairing of a Job with a JobSchedule that tells it when to execute. This is
 * the basic operational unit that JobSchedulers will deal with.
 */
public struct ScheduledJob {
    public CurrentTimeProvider timeProvider;
    public JobSchedule schedule;
    public Job job;
    public long id;
}

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
        auto sj = ScheduledJob(this.getTimeProvider(), schedule, job, this.getNextScheduledJobId());
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
     * Adds a job to the scheduler, whose schedule is defined by the given cron
     * expression string.
     * Params:
     *   fn = A function to execute.
     *   cronExpressionString = A Cron expression string defining when to run the job.
     * Returns: The scheduled job.
     */
    public final ScheduledJob addCronJob(void function() fn, string cronExpressionString) {
        return addJob(new FunctionJob(fn), new CronSchedule(cronExpressionString));
    }

    /** 
     * Gets this scheduler's time provider.
     * Returns: The scheduler's time provider.
     */
    protected CurrentTimeProvider getTimeProvider();

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
     * Stops the scheduler. This method blocks until shutdown is complete.
     * Params:
     *   force = Whether to forcibly shutdown, cancelling any current jobs.
     */
    public void stop(bool force);

    /** 
     * Stops the scheduler, and waits for any currently-executing jobs to
     * finish. Equivalent to calling stop(false).
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

// For testing, we provide a standardized test suite for an Scheduler to ensure
// that it is compliant to the interface(s).
// It is therefore recommended that all implementations of JobScheduler call
// `testScheduler(() => new MyCustomScheduler());` in their unit test.
version(unittest) {
    import scheduled.schedules;
    import std.datetime;
    import slf4d;

    alias SchedulerFactory = JobScheduler delegate();

    public void testScheduler(SchedulerFactory factory) {
        import std.string : format;
        auto sampleScheduler = factory();
        string name = format!"%s"(sampleScheduler);
        sampleScheduler.stop();
        auto log = getLogger();

        log.infoF!"Testing %s.addJob(...)"(name);
        testAddJob(factory);
        log.infoF!"Testing %s.getNextScheduledJobId()"(name);
        testGetNextScheduledJobId(factory);
        log.infoF!"Testing %s.start()"(name);
        testStart(factory);
        log.infoF!"Testing %s.stop()"(name);
        testStop(factory);
    }

    private void testAddJob(SchedulerFactory factory) {
        auto scheduler = factory();
        IncrementJob job = new IncrementJob();
        auto scheduledJob = scheduler.addJob(job, new FixedIntervalSchedule(msecs(10)));
        assert(job.x == 0);
        assert(scheduledJob.job == job);

        auto anotherScheduledJob = scheduler.addJob(job, new FixedIntervalSchedule(msecs(20)));
        assert(anotherScheduledJob.job == job);
        assert(anotherScheduledJob.id != scheduledJob.id);
    }

    private void testGetNextScheduledJobId(SchedulerFactory factory) {
        import std.algorithm : canFind;
        auto scheduler = factory();
        IncrementJob job = new IncrementJob();
        ulong[] ids = [];
        for (int i = 0; i < 1000; i++) {
            ulong newId = scheduler.getNextScheduledJobId();
            assert(!canFind(ids, newId));
            ids ~= newId;
            scheduler.addJob(job, new FixedIntervalSchedule(msecs(5)));
        }
    }

    /** 
     * Tests the `start()` function of the scheduler, and therefore also the
     * main operation of the scheduler to execute jobs after starting.
     * Params:
     *   factory = The scheduler factory.
     */
    private void testStart(SchedulerFactory factory) {
        import core.thread : Thread;

        auto scheduler = factory();
        IncrementJob jobA = new IncrementJob();
        scheduler.addJob(jobA, new FixedIntervalSchedule(msecs(5)));
        IncrementJob jobB = new IncrementJob();
        scheduler.addJob(jobB, new FixedIntervalSchedule(msecs(10)));
        IncrementJob jobC = new IncrementJob();
        scheduler.addJob(jobC, new OneTimeSchedule(msecs(5)));

        // Before starting, all jobs should not have ran.
        assert(jobA.x == 0);
        assert(jobB.x == 0);
        assert(jobC.x == 0);

        scheduler.start();
        Thread.sleep(msecs(1));
        // After starting and running a bit, both interval jobs should now have ran once.
        assert(jobA.x == 1);
        assert(jobB.x == 1);

        Thread.sleep(msecs(11));
        // After a total of 12ms, jobA should have ran 3 times at t=0, 5, 10, and jobB 2 times at t=0, 10.
        assert(jobA.x == 3);
        assert(jobB.x == 2);
        // jobC should have ran once, and not been re-queued.
        assert(jobC.x == 1);

        scheduler.stop();
    }

    private void testStop(SchedulerFactory factory) {
        import core.thread : Thread;

        // Test stopping and waiting for tasks to finish.
        auto scheduler = factory();
        LongIncrementJob jobA = new LongIncrementJob(msecs(5));
        scheduler.addJob(jobA, new FixedIntervalSchedule(msecs(10)));
        assert(jobA.x == 0);
        scheduler.start();
        Thread.sleep(msecs(1));
        scheduler.stop(false);
        assert(jobA.x == 1);

        // Test forcefully stopping.
        scheduler = factory();
        LongIncrementJob jobB = new LongIncrementJob(msecs(1000));
        scheduler.addJob(jobB, new FixedIntervalSchedule(seconds(5)));
        assert(jobB.x == 0);
        scheduler.start();
        Thread.sleep(msecs(1));
        scheduler.stop(true);
        assert(jobB.x == 0);
    }

    private class IncrementJob : Job {
        public uint x = 0;
        public void run() {
            x++;
        }
    }

    private class LongIncrementJob : IncrementJob {
        private Duration dur;

        public this(Duration dur = msecs(5)) {
            this.dur = dur;
        }

        public override void run() {
            import core.thread : Thread;
            Thread.sleep(this.dur);
            super.run();
        }
    }
}
