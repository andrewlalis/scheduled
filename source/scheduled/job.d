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
 * Represents a pairing of a Job with a schedule.
 */
public final class ScheduledJob {
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
     * Constructs a new pairing of a job and a schedule.
     * Params:
     *   job = The job which is scheduled.
     *   schedule = The schedule that defines when the job will run.
     *   timeProvider = Provider of current timestamps.
     */
    public this(Job job, JobSchedule schedule, CurrentTimeProvider timeProvider) {
        this.job = job;
        this.schedule = schedule;
        this.timeProvider = timeProvider;
    }

    /** 
     * Constructs a new pairing of a job and a schedule, with the default
     * system time provider.
     * Params:
     *   job = The job which is scheduled.
     *   schedule = The schedule that defines when the job will run.
     */
    public this(Job job, JobSchedule schedule) {
        this(job, schedule, new SysTimeProvider);
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
     * Compares two scheduled jobs, such that jobs whose next execution time
     * is earlier, are considered greater than others.
     * Params:
     *   other = The object to compare to.
     * Returns: 1 if this object is greater, -1 if this object is less, or 0
     * otherwise.
     */
    override int opCmp(Object other) {
        if (auto otherJob = cast(ScheduledJob) other) {
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

    ScheduledJob jobA = new ScheduledJob(j, s1);
    ScheduledJob jobA2 = new ScheduledJob(j, s1);
    ScheduledJob jobB = new ScheduledJob(j, s2);
    ScheduledJob jobC = new ScheduledJob(j, s3);
    assert(jobA > jobB);
    assert(jobA >= jobA2 && jobA <= jobA2);
    assert(jobB > jobC);
    assert(jobA > jobC);
}
