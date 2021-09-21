module scheduled.schedule;

import std.datetime;
import std.typecons;

/** 
 * A schedule that governs when a job should be executed.
 */
public interface JobSchedule {
    Nullable!SysTime getNextExecutionTime(SysTime currentTime) const;

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

/** 
 * Simple schedule in which a job is executed once per some fixed duration,
 * after a specified starting time.
 */
public class FixedIntervalSchedule : JobSchedule {
    import core.time;

    private Duration interval;
    private SysTime start;
    private ulong elapsedIntervals;

    this(Duration interval, SysTime start) {
        this.interval = interval;
        this.start = start;
        this.elapsedIntervals = 0;
    }

    this(Duration interval) {
        this(interval, Clock.currTime);
    }

    Nullable!SysTime getNextExecutionTime(SysTime currentTime) const {
        return (this.start + (this.elapsedIntervals * this.interval)).nullable;
    }

    void markExecuted(SysTime executionTime) {
        this.elapsedIntervals++;
    }

    bool isRepeating() {
        return true;
    }
}
