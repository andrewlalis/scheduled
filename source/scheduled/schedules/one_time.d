module scheduled.schedules.one_time;

import std.datetime;
import std.typecons;
import scheduled.schedule;

/** 
 * Schedule which defines a job to be executed once at a fixed time.
 */
public class OneTimeSchedule : JobSchedule {
    private SysTime executionTime;
    private bool executed = false;

    this(SysTime executionTime) {
        this.executionTime = executionTime;
    }

    this(Duration timeUntilExecution) {
        this(Clock.currTime + timeUntilExecution);
    }

    Nullable!SysTime getNextExecutionTime(SysTime currentTime) const {
        Nullable!SysTime result;
        if (this.executed) {
            result.nullify;
        } else {
            result = this.executionTime;
        }
        return result;
    }

    void markExecuted(SysTime executionTime) {
        this.executed = true;
    }

    bool isRepeating() {
        return false;
    }
}