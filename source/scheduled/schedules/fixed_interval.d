module scheduled.schedules.fixed_interval;

import core.time;
import std.datetime;
import std.typecons;

import scheduled.schedule;

/** 
 * Simple schedule in which a job is executed once per some fixed duration,
 * after a specified starting time.
 */
public class FixedIntervalSchedule : JobSchedule {
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

    bool isRepeating() const {
        return true;
    }
}