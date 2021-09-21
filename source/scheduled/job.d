module scheduled.job;

import std.datetime;
import scheduled.schedule;

/**
 * A job is a task which is submitted to the scheduler, to be run one or more
 * times, according to a given schedule.
 */
public interface Job {
    public void run();
}

/** 
 * Simple job that executes a function.
 */
public class FunctionJob : Job {
    private void function() fn;

    this(void function() fn) {
        this.fn = fn;
    }

    override public void run() {
        this.fn();
    }
}

/** 
 * Represents a pairing of a Job with a schedule.
 */
public final class ScheduledJob {
    import std.stdio;

    private const CurrentTimeProvider timeProvider;
    private JobSchedule schedule;
    private Job job;

    public this(Job job, JobSchedule schedule, CurrentTimeProvider timeProvider) {
        this.job = job;
        this.schedule = schedule;
        this.timeProvider = timeProvider;
    }

    public this(Job job, JobSchedule schedule) {
        this(job, schedule, new SysTimeProvider);
    }

    public JobSchedule getSchedule() {
        return this.schedule;
    }

    public Job getJob() {
        return this.job;
    }

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
