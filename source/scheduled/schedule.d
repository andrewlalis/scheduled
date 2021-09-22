module scheduled.schedule;

import std.datetime;
import std.typecons;

/** 
 * A schedule that governs when a job should be executed.
 */
public interface JobSchedule {
    /**
     * Gets the timestamp at which the scheduler should plan to execute a job
     * with this schedule next.
     */
    Nullable!SysTime getNextExecutionTime(SysTime currentTime);

    /**
     * Marks the schedule as having been executed at the given time.
     */
    void markExecuted(SysTime executionTime);

    /**
     * Tells whether the schedule is repeating; that jobs with this schedule
     * should be re-queued after being executed.
     * Returns: True if this schedule is repeating, or false otherwise.
     */
    bool isRepeating();
}

/**
 * Simple wrapper interface for obtaining the current system time.
 */
public interface CurrentTimeProvider {
    /** 
     * Gets the current system time.
     * Returns: The current system time.
     */
    SysTime now() const;
}

/** 
 * Standard implementation of the current time provider, which simply returns
 * the current system time.
 */
public class SysTimeProvider : CurrentTimeProvider {
    SysTime now() const {
        return Clock.currTime;
    }
}

/** 
 * Implementation of the current time provider which always returns a fixed
 * value, useful for testing.
 */
public class FixedTimeProvider : CurrentTimeProvider {
    private const SysTime currentTime;

    this(SysTime currentTime) {
        this.currentTime = currentTime;
    }

    SysTime now() const {
        return this.currentTime;
    }
}
