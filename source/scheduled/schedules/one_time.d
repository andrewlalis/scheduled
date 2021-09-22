module scheduled.schedules.one_time;

import std.datetime;
import std.typecons;
import scheduled.schedule;

/** 
 * Schedule which defines a job to be executed once at a fixed time.
 *
 * Note that if a job is scheduled with this schedule, and the execution time
 * is set to a time in the past, the job will be executed immediately.
 */
public class OneTimeSchedule : JobSchedule {
    /**
     * The time at which the job will be executed.
     */
    private SysTime executionTime;

    /** 
     * Simple flag to indicate whether the job has already been executed. This
     * is useful because 
     */
    private bool executed = false;

    /** 
     * Constructs a one-time schedule to execute at the specified time.
     * Params:
     *   executionTime = The time to execute the job at.
     */
    this(SysTime executionTime) {
        this.executionTime = executionTime;
    }

    /** 
     * Constructs a one-time schedule to execute after the given duration has
     * passed, from the current system time.
     * Params:
     *   timeUntilExecution = The time from now until execution.
     */
    this(Duration timeUntilExecution) {
        this(Clock.currTime + timeUntilExecution);
    }

    /** 
     * Gets the next execution time. If this one-time schedule has already
     * executed once, it will always return null.
     * Params:
     *   currentTime = The current system time.
     * Returns: The time at which jobs with this schedule should next be
     * executed, or null if there is no known next execution time.
     */
    Nullable!SysTime getNextExecutionTime(SysTime currentTime) const {
        if (this.executed) {
            return Nullable!SysTime.init;
        } else {
            return nullable!SysTime(this.executionTime);
        }
    }

    /** 
     * Marks this schedule as having been executed. Once this is called, no
     * more jobs with this schedule will be executed.
     * Params:
     *   executionTime = The time at which this schedule's job was executed.
     */
    void markExecuted(SysTime executionTime) {
        this.executed = true;
    }

    /** 
     * Tells the scheduler that this schedule is never repeating.
     * Returns: Always false.
     */
    bool isRepeating() {
        return false;
    }
}