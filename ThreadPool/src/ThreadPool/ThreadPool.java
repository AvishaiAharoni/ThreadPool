package ThreadPool;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import WaitablePQueue.WaitablePQueue;

/**
 * @author Avishai
 *
 */
public class ThreadPool implements Executor {
	private int numOfThreads;
	private WaitablePQueue<Task<?>> taskPool;
	private ArrayList<Thread> threadsArr;
	private Semaphore semPause;
	private Semaphore semTerminate;
	private final int SUPER;
	private final int LAST;
	private boolean isPause;
	private boolean canSubmit;
	private boolean shouldRun;

	/**
	 * constructor to init the vars
	 * @param numOfThreads - number of threads to run
	 */
	public ThreadPool(int numOfThreads) {
		this.numOfThreads = numOfThreads;
		this.taskPool = new WaitablePQueue<>();
		this.threadsArr = new ArrayList<>();
		this.canSubmit = true;
		this.semPause = new Semaphore(0);
		this.semTerminate = new Semaphore(0);
		this.SUPER = Integer.MAX_VALUE;
		this.LAST = Integer.MIN_VALUE;

		addThreads(numOfThreads);
	}

	/**
	 * to start all the threads to run
	 */
	public void runThreads() {
		if (!this.shouldRun) {
			this.shouldRun = true;
			for (Thread th : threadsArr) {
				th.start();
			}
		}
	}

	@Override
	public void execute(Runnable command) {
		if (this.shouldRun) {
			command.run();
		}
	}

	/**
	 * to pause the tasks.
	 * task that in progress continues, but new tasks doesn't start.
	 * can't pause after: pause, shut down and await termination.
	 */
	public void pause() {
		if ((this.canSubmit) && (!this.isPause) && (this.shouldRun)) {
			this.semPause.drainPermits();
			this.isPause = true;
			insertTasksToPause(this.numOfThreads);
		}
	}		

	/**
	 * to resume after pause.
	 */
	public void resume() {		
		this.semPause.release(this.numOfThreads);
		this.isPause = false;
	}		

	/**
	 * to submit a new task to the taskPool
	 * @param run - a Runnable function
	 * @param p - the priority of the task
	 * @return the future return value of the task
	 */
	public Future<Void> submit(Runnable run, Priority p) {
		return submitTask(Executors.callable(run, null), p.ordinal());
	}		

	/**
	 * to submit a new task to the taskPool
	 * @param run - a Runnable function
	 * @param p - the priority of the task
	 * @param val - the return value of the task
	 * @return the future return value of the task
	 */
	public <T> Future<T> submit(Runnable run, Priority p, T val) {		
		return submitTask(Executors.callable(run, val), p.ordinal());
	}		

	/**
	 * to submit a new task to the taskPool
	 * @param call - a callable function
	 * @return the future return value of the task
	 */
	public <T> Future<T> submit(Callable<T> call) {
		return submitTask(call, Priority.getMediumValue());	
	}

	/**
	 * to submit a new task to the taskPool
	 * @param call - a callable function
	 * @param p - the priority of the task
	 * @return the future return value of the task
	 */
	public <T> Future<T> submit(Callable<T> call, Priority p) {		
		return submitTask(call, p.ordinal());	
	}

	/**
	 * to set a new num of the threads that work (with the same status of the prev threads)
	 * @param newNum - the new num of the threads that work on the tasks
	 * if the number of newNum is negative - undefine behaviour.
	 */
	public void setNumOfThreads(int newNum) {
		checkForSubmit();
		
		if (newNum < 0) {
			throw new IllegalArgumentException();
		}

		int diff = newNum - this.numOfThreads;

		// need to add threads
		if (diff >= 0) {
			// case of adding threads when they are in pouse mode
			if (this.isPause) {
				insertTasksToPause(diff);
			}

			addThreads(diff);
		}
		// need to sub threads
		else {
			diff = -diff;

			// for case of set before runThreads
			if (!this.shouldRun) {
				removeThreads(diff);
			}
			else {					//regular case
				insertTaskToStopThread(diff, this.SUPER);
				this.semPause.release(diff);
			}
		}

		this.numOfThreads = newNum;
	}

	/**
	 * to get the number of the threads in the threadArr.
	 * @return the number of the threads.
	 */
	public int getNumOfRunningThreads() {
		return this.threadsArr.size();
	}

	/**
	 * to get the number of the tasks in the {@link WaitablePQueue}.
	 * @return the number of the tasks.
	 */
	public int getNumOfTasks() {
		return this.taskPool.getSize();
	}

	/**
	 * to shut down all the work.
	 * all task that in progress continue in the background.
	 */
	public void shutDown() {
		shut(this.LAST);
	}

	/**
	 * to shut down all the work.
	 * task that in progress continues, but new tasks doesn't start.
	 * after finishing the current tasks - shut down.
	 */
	public void shutDownNow() {
		shut(this.SUPER);
	}

	/**
	 * to shut down all the work, but after finishing all the tasks.
	 * all the tasks are execting.
	 * after finishing the tasks - shut down.
	 * for doing await termination before run - starting the threads to run.
	 */
	public void awaitTermination() {
		runThreads();
		this.semTerminate.drainPermits();
		stopThreadsToShut(this.LAST);
		try {
			this.semTerminate.acquire(this.numOfThreads);
		} catch (InterruptedException e) { }
	}

	/**
	 * to shut down all the work in given time, but after trying to finish all the tasks.
	 * after finishing the tasks - shut down, or after the given time
	 * for doing await termination before run - starting the threads to run.
	 * @param timeout - the time to wait before the shut down.
	 * @param unit - the unit of the given time.
	 * @return <code>true</code> if all the tasks was finished before the end of the time,
	 * otherwithe - <code>false</code>.
	 */
	public boolean awaitTermination(long timeout, TimeUnit unit) {
		runThreads();
		this.semTerminate.drainPermits();
		stopThreadsToShut(this.LAST);
		try {
			// false - the time is over, true - the semaphore aquire
			if (!this.semTerminate.tryAcquire(this.numOfThreads, timeout, unit)) {
				insertTaskToStopThread(this.threadsArr.size(), this.SUPER);
			}
		} catch (InterruptedException e) { }

		return this.taskPool.isEmpty();
	}

	/*******************************************************************/
	/**
	 * class for the tasks in the WaitablePQueue
	 * @author Me
	 *
	 * @param <T>
	 */
	private class Task<T> implements Comparable<Task<T>> {
		private Integer priority;
		private final Callable<T> func;
		private final FutureBox future;

		/**
		 * constructor
		 * @param priority - for the task
		 * @param call - the function to execute
		 */
		Task(Integer priority, Callable<T> call) {
			this.priority = priority;
			this.func = call;
			this.future = new FutureBox(this);
		}

		/* 
		 * a compare function (by the priorities)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(Task<T> task) {
			return task.priority - this.priority;
		}

		/**
		 * to execute the {@link Callable} function in the task
		 * @throws Exception - of the function
		 */
		private void excuteMe() throws Exception {
			this.future.setVal(this.func.call());
		}

		/**
		 * to get the {@link Future} result from the task
		 * @return the future result
		 */
		private Future<T> getFuture() {
			return this.future;
		}


		/*******************************************************************/
		/**
		 * a class for the {@link Future} interface
		 * @author Me
		 *
		 * @param <T>
		 */
		private class FutureBox implements Future<T> { 
			private T retVal;
			private boolean wasCancelled;
			private boolean wasDone;
			private boolean wasException;
			private Semaphore semVal;
			private final Task<T> task;

			/**
			 * constructor
			 * @param task - the task from the {@link WaitablePQueue}
			 */
			private FutureBox(Task<T> task) {
				this.task = task;
				this.semVal = new Semaphore(0);
			}

			/* 
			 * to try to cancell the current task.
			 * @see java.util.concurrent.Future#cancel(boolean)
			 * @param mayInterruptIfRunning - true if the thread should be interrupt
			 * @return true if the task cancelled.
			 * false if the task has already completed, has already been cancelled,
			 * or could not be cancelled for some other reason.
			 */
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				try {
					if (ThreadPool.this.taskPool.remove(task)) {
						this.wasCancelled = true;
						this.wasDone = true;

						return true;
					}
				} catch (InterruptedException e) {
					this.wasException = true;
					this.wasDone = true;
				}

				return false;
			}

			/* 
			 * to check if the task was cancelled
			 * @see java.util.concurrent.Future#isCancelled()
			 * @return true if this task was cancelled before it completed normally.
			 */
			@Override
			public boolean isCancelled() {
				return this.wasCancelled;
			}

			/* 
			 * to check if the task was done
			 * @see java.util.concurrent.Future#isDone()
			 * @return true if the task completed.
			 * Completion may be due to normal termination, an exception, or cancellation.
			 */
			@Override
			public boolean isDone() {
				return this.wasDone;
			}

			/* 
			 * to get the retVal of the task
			 * @see java.util.concurrent.Future#get()
			 * @Throws:
			 * InterruptedException - if the current thread was interrupted while waiting
			 * ExecutionException - if the computation threw an exception
			 */
			@Override
			public T get() throws InterruptedException, ExecutionException {
				checkForException();

				this.semVal.acquire();
				this.semVal.release();

				return this.retVal;
			}

			/* 
			 * to wait at most the given time for the computation to complete, and then retrieves its result, if available.
			 * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
			 * @Throws:
			 * InterruptedException - if the current thread was interrupted while waiting
			 * ExecutionException - if the computation threw an exception
			 * TimeoutException - if the wait timed out
			 */
			@Override
			public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
				checkForException();

				// false - the time is over, true - the semaphore aquire
				if (this.semVal.tryAcquire(timeout, unit)) {
					this.semVal.release();
				}

				return this.retVal;
			}

			/**
			 * to init the ret val
			 * @param val - the ret val
			 */
			private void setVal(T val) {
				this.retVal = val;
				this.semVal.release();
				this.wasDone = true;
			}

			/**
			 * help method to check if the isCancelled flag is true.
			 * if true throws {@link CancellationException}.
			 * @throws InterruptedException 
			 */
			private void checkForException() throws InterruptedException {
				if (this.isCancelled()) {	
					throw new CancellationException();
				}
				if (this.wasException) {
					throw new InterruptedException();
				}
			}
		}
	}

	/**
	 * the priorities of the tasks.
	 * @author me
	 *
	 */
	public enum Priority {
		LOW, MEDIUM, HIGH;
				
		/**
		 * to get the medium priority (in the middle)
		 * @return the medium priority
		 */
		private static int getMediumValue() {
			return Priority.values().length / 2;
		}
	}

	/***********************************************************/
	/**********************help methods*************************/
	/***********************************************************/

	/**
	 * to add new threads to the {@link ArrayList} of the threads
	 * @param numOfThreads
	 */
	private void addThreads(int numOfThreads) {
		// the method for each thread
		Runnable run = () -> {					// lambda of runnable
			while (!Thread.interrupted()) {
				try {
					taskPool.dequeue().excuteMe();
				} catch (Exception e) {	}
			}

			// synchronize to ensure that every thread doing remove
			synchronized (this.threadsArr) {
				threadsArr.remove(Thread.currentThread());
				this.semTerminate.release();
			}
		};

		// to insert the threads to the ArrList
		for (int i = 0; i < numOfThreads; ++i) {
			Thread t = new Thread(run);
			this.threadsArr.add(t);

			// for set afer run
			if (this.shouldRun) {
				t.start();
			}
		}
	}

	/**
	 * to create and insert new tasks to the queue that pause the threads
	 * @param numOfTasks - the number of the tasks to insert
	 * @param priority - the priority of the tasks (SUPER)
	 */
	private void insertTasksToPause(int numOfTasks) {
		Callable<Void> call = () -> {				// lambda of callable
			try {
				semPause.acquire();
			} catch (InterruptedException e) {	}

			return null;	
		};

		insertTasks(call, numOfTasks, this.SUPER);
	}

	/**
	 * to create and insert new tasks to the queue that stop the threads
	 * @param numOfTasks - the number of the tasks to insert
	 * @param priority - the priority of the tasks
	 */
	private void insertTaskToStopThread(int numOfTasks, int priority) {
		Callable<Void> call = () -> {				// lambda of callable
			Thread.currentThread().interrupt();

			return null;	
		};

		insertTasks(call, numOfTasks, priority);
	}

	/**
	 * to insert tasks (with the given priority) to interrupt all the threads
	 * @param priority - the priority of the tasks.
	 * fron shut down - a SUPER priority.
	 * from await terminate - a LAST priority.
	 */
	private void stopThreadsToShut(int priority) {
		this.canSubmit = false;

		insertTaskToStopThread(this.numOfThreads, priority);

		this.semPause.release(this.numOfThreads);
	}

	/**
	 * to check if the canSubmit flag is false.
	 * if false throws {@link RejectedExecutionException}.
	 */
	private void checkForSubmit() {
		if (!this.canSubmit) {	
			throw new RejectedExecutionException();
		}	
	}

	/**
	 * to insert numOfTasks times a call method with a given priority.
	 * @param call - the method to insert to the tasks
	 * @param numOfTasks - the number of the tasks
	 * @param priority - the priority of the tasks
	 */
	private void insertTasks(Callable<Void> call, int numOfTasks, int priority) {
		for (int i = 0; i < numOfTasks; ++i) {
			this.taskPool.enqueue(new Task<>(priority, call));
		}
	}
	
	/**
	 * to remove threads immediately and not by tasks.
	 * this method can be used only before the runThreads was called.
	 * @param numOfThreads - the number of the threads that need to remove.
	 */
	private void removeThreads(int numOfThreads) {
		// running from the last index backwords
		final int lastIdx = this.threadsArr.size() - 1;
		for (int i = lastIdx; i > lastIdx - numOfThreads; --i) {
			this.threadsArr.remove(i);
		}
	}
	
	/**
	 * to submit a new task to the taskPool
	 * @param call - a callable function
	 * @param pri - the priority of the task - in integer
	 * @return the future return value of the task
	 */
	private <T> Future<T> submitTask(Callable<T> call, int pri) {		
		checkForSubmit();

		Task<T> task = new Task<>(pri, call);
		this.taskPool.enqueue(task);
		
		return task.getFuture();	
	}
	
	/**
	 * to shut down all the work.
	 * for shutDown - shut down, and all the tasks that in the {@link WaitablePQueue} continue in the background.
	 * for shutDownNownew - new tasks doesn't start, and after finishing the current tasks - shut down.
	 * @param pri - the priority of the task to insert - in integer
	 */
	private void shut(int pri) {
		if (this.shouldRun) {
			stopThreadsToShut(pri);
		}
		else {			// for case of shut down before run
			this.threadsArr.clear();
		}
	}
}