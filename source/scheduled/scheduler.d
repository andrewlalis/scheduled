module scheduled.scheduler;

import scheduled.job;
import scheduled.schedule;

/** 
 * A scheduler is the core component of the library; you add jobs to the job
 * scheduler, and then it will execute these according to the job's schedule.
 */
public interface JobScheduler {
    void addJob(ScheduledJob job);

    final void addJob(Job job, JobSchedule schedule) {
        addJob(new ScheduledJob(job, schedule));
    }

    final void addJob(void function() fn, JobSchedule schedule) {
        addJob(new FunctionJob(fn), schedule);
    }

    void start();

    void stop(bool force);

    /** 
     * Stops the scheduler, and waits for any currently-executing jobs to
     * finish. Functionally equivalent to calling stop(false).
     */
    final void stop() {
        stop(false);
    }
}

import core.thread;

/** 
 * A simple thread-based scheduler that sleeps until the next task, and runs it
 * using a task pool.
 */
public class ThreadedJobScheduler : Thread, JobScheduler {
    import std.parallelism;
    import std.container.binaryheap;
    import std.datetime.systime;
    import core.time;

    /** 
     * The maximum amount of time that this scheduler may sleep for. This is
     * mainly used as a sanity check against clock deviations or other
     * inconsistencies in timings.
     */
    private static immutable Duration MAX_SLEEP_TIME = seconds(60);

    private CurrentTimeProvider timeProvider;
    private TaskPool taskPool;
    private BinaryHeap!(ScheduledJob[]) jobPriorityQueue;
    private shared bool running;

    public this(TaskPool taskPool, CurrentTimeProvider timeProvider) {
        super(&this.run);
        this.taskPool = taskPool;
        this.timeProvider = timeProvider;
        this.jobPriorityQueue = BinaryHeap!(ScheduledJob[])([]);
    }

    public this() {
        this(std.parallelism.taskPool(), new SysTimeProvider);
    }

    void addJob(ScheduledJob job) {
        if (this.running) throw new Exception("Cannot add tasks while the scheduler is running.");
        this.jobPriorityQueue.insert(job);
    }

    void start() {
        super.start();
    }

    /** 
     * Runs the scheduler. This works by popping the next scheduled task from
     * the priority queue (since scheduled tasks are ordered by their next
     * execution date) and sleeping until we reach that task's execution date.
     */
    void run() {
        this.running = true;
        while (this.running && !this.jobPriorityQueue.empty) {
            ScheduledJob job = this.jobPriorityQueue.front;
            this.jobPriorityQueue.removeFront;
            SysTime now = this.timeProvider.now;
            auto nextExecutionTime = job.getSchedule.getNextExecutionTime(now);
            // If the job doesn't have a next execution, skip it, don't requeue it, and try again.
            if (nextExecutionTime.isNull) continue;
            Duration timeUntilJob = hnsecs(nextExecutionTime.get.stdTime - now.stdTime);
            
            // If the time until the next job is longer than our max sleep time, requeue the job and sleep as long as possible.
            if (MAX_SLEEP_TIME < timeUntilJob) {
                this.jobPriorityQueue.insert(job);
                this.sleep(MAX_SLEEP_TIME);
            } else {
                // The time until the next job is close enough that we can sleep directly to it.
                if (timeUntilJob > hnsecs(0)) {
                    this.sleep(timeUntilJob);
                }
                // Queue up running the job, and process all other aspects of it.
                this.taskPool.put(task(&job.getJob.run));
                job.getSchedule.markExecuted(this.timeProvider.now);
                if (job.getSchedule.isRepeating) {
                    this.jobPriorityQueue.insert(job);
                }
            }
        }
    }

    void stop(bool force) {
        this.running = false;
        this.taskPool.finish(true);
    }
}
