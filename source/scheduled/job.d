/**
 * Defines the job interface, and some simple utilities to go with it.
 */
module scheduled.job;

import std.datetime;
import scheduled.schedule;

/**
 * A job is a task which is submitted to the scheduler, to be run one or more
 * times, according to a given schedule.
 */
public interface Job {
    /**
     * The method which is called to execute this job.
     */
    public void run();
}

/** 
 * Simple job that executes a function.
 */
public class FunctionJob : Job {
    /** 
     * The function to execute when this job is run.
     */
    private void function() fn;

    /** 
     * Constructs a job that will run the given function.
     * Params:
     *   fn = The function to execute.
     */
    this(void function() fn) {
        this.fn = fn;
    }

    /** 
     * Runs the function.
     */
    override public void run() {
        this.fn();
    }
}

/** 
 * Represents a pairing of a Job with a schedule. This is a component that
 * is utilized internally by Schedulers.
 */
package final class ScheduledJob {
    /** 
     * The component which is used to obtain current timestamps.
     */
    private const CurrentTimeProvider timeProvider;

    /** 
     * The schedule which defines when the associated job will run.
     */
    private JobSchedule schedule;

    /** 
     * The job which will run according to the associated schedule.
     */
    private Job job;

    /** 
     * The unique id for this scheduled job, within the context of the
     * scheduler that's responsible for it. This id should be unique among all
     * jobs currently managed by the scheduler, but ids may be reused once old
     * jobs are removed.
     */
    private immutable long id;

    /** 
     * Constructs a new pairing of a job and a schedule.
     * Params:
     *   job = The job which is scheduled.
     *   schedule = The schedule that defines when the job will run.
     *   id = The unique id for this job.
     *   timeProvider = Provider of current timestamps.
     */
    package this(Job job, JobSchedule schedule, long id, CurrentTimeProvider timeProvider) {
        this.job = job;
        this.schedule = schedule;
        this.timeProvider = timeProvider;
        this.id = id;
    }

    /** 
     * Constructs a new pairing of a job and a schedule, with the default
     * system time provider.
     * Params:
     *   job = The job which is scheduled.
     *   schedule = The schedule that defines when the job will run.
     *   id = The unique id for this job.
     */
    package this(Job job, JobSchedule schedule, long id) {
        this(job, schedule, id, new SysTimeProvider);
    }

    /** 
     * Gets the schedule from this pairing.
     * Returns: The schedule.
     */
    public JobSchedule getSchedule() {
        return this.schedule;
    }

    /** 
     * Gets the job from this pairing.
     * Returns: The job.
     */
    public Job getJob() {
        return this.job;
    }

    /** 
     * Gets the id for this scheduled job.
     * Returns: The id.
     */
    public long getId() {
        return this.id;
    }

    /** 
     * Compares two scheduled jobs, such that jobs whose next execution time
     * is earlier, are considered greater than others.
     * Params:
     *   other = The object to compare to.
     * Returns: 1 if this object is greater, -1 if this object is less, or 0
     * otherwise.
     */
    override int opCmp(Object other) {
        if (auto otherJob = cast(ScheduledJob) other) {
            if (this.getId() == otherJob.getId()) return 0; // Exit right away if we're comparing to the same scheduled job.
            SysTime now = timeProvider.now;
            auto t1 = this.getSchedule().getNextExecutionTime(now);
            auto t2 = otherJob.getSchedule().getNextExecutionTime(now);
            if (t1.isNull && t2.isNull) return 0;
            if (!t1.isNull && t2.isNull) return 1;
            if (t1.isNull) return -1;
            return t2.get.opCmp(t1.get);
        } else {
            return 0;
        }
    }
}

/**
 * Tests the functionality of comparing two scheduled jobs.
 */
unittest {
    import std.experimental.logger;
    import scheduled.schedules.one_time;

    JobSchedule s1 = new OneTimeSchedule(msecs(50));
    JobSchedule s2 = new OneTimeSchedule(msecs(500));
    JobSchedule s3 = new OneTimeSchedule(msecs(2500));

    class IncrementJob : Job {
        public uint x = 0;
        public void run() {
            x++;
            logf(LogLevel.info, "Incrementing counter to %d.", x);
        }
    }
    auto j = new IncrementJob;

    ScheduledJob jobA = new ScheduledJob(j, s1, 1);
    ScheduledJob jobA2 = new ScheduledJob(j, s1, 2);
    ScheduledJob jobB = new ScheduledJob(j, s2, 3);
    ScheduledJob jobC = new ScheduledJob(j, s3, 4);
    assert(jobA > jobB);
    assert(jobA >= jobA2 && jobA <= jobA2);
    assert(jobB > jobC);
    assert(jobA > jobC);
}
