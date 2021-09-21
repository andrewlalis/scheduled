module scheduled.schedules.daily;

import std.datetime;
import std.typecons;
import std.algorithm;
import std.range;
import scheduled.schedule;

/** 
 * Schedule which executes jobs at one or more fixed times each day.
 */
public class DailySchedule : JobSchedule {
    private const TimeOfDay[] executionTimes;

    this(TimeOfDay[] executionTimes) {
        assert(!executionTimes.empty, "Daily schedule requires at least one execution time.");
        executionTimes.sort;
        this.executionTimes = executionTimes;
    }

    this(TimeOfDay singleExecutionTime) {
        this([singleExecutionTime]);
    }

    this(string[] isoExtStrings) {
        this(isoExtStrings.map!(s => TimeOfDay.fromISOExtString(s)).array);
    }

    this(string isoExtString) {
        this([isoExtString]);
    }

    Nullable!SysTime getNextExecutionTime(SysTime currentTime) {
        TimeOfDay currentTimeOfDay = cast(TimeOfDay)currentTime.toLocalTime;
        Date currentDate = cast(Date)currentTime.toLocalTime;
        foreach (executionTime; executionTimes) {
            if (executionTime > currentTimeOfDay) {
                return (SysTime(DateTime(currentDate, executionTime))).nullable;
            }
        }
        // Could not find an execution time for the current day, so choose the first time tomorrow.
        // Execution times are guaranteed to be sorted.
        return (SysTime(DateTime(currentDate + days(1), executionTimes[0]))).nullable;
    }

    void markExecuted(SysTime executionTime) {
        // Don't do anything.
    }

    bool isRepeating() {
        return true;
    }
}