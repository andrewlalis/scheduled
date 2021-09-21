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

    private static immutable ulong MAX_SLEEP_MSECS = 5000;

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

    void run() {
        this.running = true;
        while (this.running && !this.jobPriorityQueue.empty) {
            ScheduledJob job = this.jobPriorityQueue.front;
            this.jobPriorityQueue.removeFront;
            SysTime now = this.timeProvider.now;
            auto nextExecutionTime = job.getSchedule.getNextExecutionTime(now);
            if (nextExecutionTime.isNull) continue;
            Duration waitTime = hnsecs(nextExecutionTime.get.stdTime - now.stdTime);
            if (waitTime > hnsecs(0)) {
                import std.stdio;
                writefln("Waiting %d ms", waitTime.total!"msecs");
                this.sleep(waitTime);
            }
            this.taskPool.put(task(&job.getJob.run));
            job.getSchedule.markExecuted(this.timeProvider.now);
            if (job.getSchedule.isRepeating) {
                this.jobPriorityQueue.insert(job);
            }
        }
    }

    void stop(bool force) {
        this.running = false;
        this.taskPool.finish(true);
    }
}
