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

    void markExecuted(SysTime executionTime);

    bool isRepeating();
}

/**
 * Simple wrapper interface for obtaining the current system time.
 */
public interface CurrentTimeProvider {
    SysTime now() const;
}

public class SysTimeProvider : CurrentTimeProvider {
    SysTime now() const {
        return Clock.currTime;
    }
}

public class FixedTimeProvider : CurrentTimeProvider {
    private const SysTime currentTime;

    this(SysTime currentTime) {
        this.currentTime = currentTime;
    }

    SysTime now() const {
        return this.currentTime;
    }
}
