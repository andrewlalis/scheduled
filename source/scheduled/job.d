/**
 * Defines the job interface.
 */
module scheduled.job;

/**
 * A job is a task which is submitted to the scheduler, to be run one or more
 * times, according to a given schedule.
 */
public interface Job {
    /**
     * The method which is called to execute this job.
     */
    public void run();
}

/** 
 * Simple job that executes a function.
 */
public class FunctionJob : Job {
    /** 
     * The function to execute when this job is run.
     */
    private void function() fn;

    /** 
     * Constructs a job that will run the given function.
     * Params:
     *   fn = The function to execute.
     */
    this(void function() fn) {
        this.fn = fn;
    }

    /** 
     * Runs the function.
     */
    override public void run() {
        this.fn();
    }
}
