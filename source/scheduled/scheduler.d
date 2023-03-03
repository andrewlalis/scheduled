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
