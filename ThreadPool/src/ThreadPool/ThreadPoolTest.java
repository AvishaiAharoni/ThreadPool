package ThreadPool;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import ThreadPool.ThreadPool.Priority;
import WaitablePQueue.WaitablePQueue;


class Counter<V> implements Callable<V>{
	int i;
	
	Counter(int i){
		this.i = i;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V call() throws Exception {
		int j = 1;
		
		while (j < 2000000000) {
			if (j % this.i == 0) {
				break;
			}
			++j;
		}
		Integer ret = i / 2;
		
		return (V)ret;
	}
}

class CounterRun<V> implements Runnable{
	int i;
	
	CounterRun(int i){
		this.i = i;
	}
	
	@Override
	public void run() {
		int j = 1;
		
		while (j < 2000000000) {
			if (j % this.i == 0) {
				break;
			}
			++j;
		}
	}
}

class Counter2 implements Runnable{
	static WaitablePQueue q = new WaitablePQueue<>();
	Integer number;
	
	Counter2(int num){
		this.number = num;
	}
	
	@Override
	public void run() {
		int j = 1;
		
		while (j < 2000000000) {
			if (j % this.number.intValue() == 0) {
				break;
			}
			++j;
		}
		
		q.enqueue(number);
	}
}


public class ThreadPoolTest {
	
	@Test
	void simpleSubmit() {
		
		ThreadPool tp = new ThreadPool(2);
		
		Integer i = 200000000;
		Integer i2 = 100000000;
		Integer i3 = 50000000;
		Integer i4 = 40000000;
		
		assertEquals(tp.getNumOfTasks(), 0);
		tp.submit(new Counter<Integer>(i), Priority.LOW);
		assertEquals(tp.getNumOfTasks(), 1);
		tp.submit(new Counter<Integer>(i2), Priority.HIGH);
		assertEquals(tp.getNumOfTasks(), 2);
		tp.submit(new Counter<Integer>(i3));
		assertEquals(tp.getNumOfTasks(), 3);
		tp.submit(new CounterRun<Integer>(i4), Priority.MEDIUM, i);
		assertEquals(tp.getNumOfTasks(), 4);
		tp.submit(new CounterRun<Integer>(i4), Priority.MEDIUM);
		assertEquals(tp.getNumOfTasks(), 5);
	}
	
	@Test
	void runThreadPool() {
		
		ThreadPool tp = new ThreadPool(5);
		
		Integer i = 200;
		Integer i2 = 100;
		Integer i3 = 500;
		Integer i4 = 600;
		Integer i5 = 700;
		
		Future<Integer> f1 = tp.submit(new Counter<Integer>(i), Priority.LOW);
		Future<Integer> f2 = tp.submit(new Counter<Integer>(i2), Priority.HIGH);
		Future<Integer> f3 = tp.submit(new Counter<Integer>(i3));
		Future<Integer> f4 = tp.submit(new CounterRun<Integer>(i4), Priority.MEDIUM, i);
		Future<Void> f5 = tp.submit(new CounterRun<Integer>(i5), Priority.MEDIUM);

		tp.runThreads();

		try {
			assertEquals(f1.get().intValue(), i / 2);
			assertEquals(f2.get().intValue(), i2 / 2);
			assertEquals(f3.get().intValue(), i3 / 2);
			assertEquals(f4.get().intValue(), i.intValue());
			assertEquals(f5.get(), null);
			assertEquals(f5.get(), null);
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	void TestPauseResume() {
		
		ThreadPool tp = new ThreadPool(3);
		
		Integer i = 210000001;
		Integer i2 = 21000002;
		Integer i3 = 210000003;
		Integer i4 = 21000004;
		
		Future<Integer> f1 = tp.submit(new Counter2(i), Priority.LOW, i);
		Future<Integer> f11 = tp.submit(new Counter2(i), Priority.LOW, i);
		Future<Integer> f12 = tp.submit(new Counter2(i), Priority.LOW, i);
		Future<Integer> f13 = tp.submit(new Counter2(i), Priority.LOW, i);
		Future<Integer> f2 = tp.submit(new Counter2(i2), Priority.MEDIUM, i);
		tp.submit(new Counter2(i2), Priority.MEDIUM, i);
		tp.submit(new Counter2(i2), Priority.MEDIUM, i);
		Future<Integer> f3 = tp.submit(new Counter2(i3), Priority.MEDIUM, i);
		tp.submit(new Counter2(i4), Priority.HIGH, i);
		tp.submit(new Counter2(i4), Priority.HIGH, i);
		tp.submit(new Counter2(i4), Priority.HIGH, i);
		
		tp.runThreads();
		
		//wait until one of medium end
		try {
			f2.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		
		tp.pause();
		
		Integer ans = 0;
		
		while(Counter2.q.getSize() > 0) {
			try {
				ans += ((Integer) Counter2.q.dequeue()).intValue() % 10;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		assertEquals(ans.intValue() > 0, true);
		
		ans = 0;
		
		tp.resume();
		
		//wait until all LOW end
		try {
			f1.get();
			f11.get();
			f12.get();
			f13.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		
		while(Counter2.q.getSize() > 0) {
			try {
				ans += ((Integer) Counter2.q.dequeue()).intValue() % 10;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		assertEquals(ans.intValue() > 0, true);
	}
	
	
	@Test
	void TestShutDown() {
		
		ThreadPool tp = new ThreadPool(3);
		
		Integer i = 210000001;
		Integer i2 = 21000002;
		Integer i3 = 2100003;
		Integer i4 = 21000004;
		
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		
		Future<Integer> f13 = tp.submit(new Counter2(i3), Priority.MEDIUM, i);
		
		tp.submit(new Counter2(i3), Priority.HIGH, i);
		tp.submit(new Counter2(i3), Priority.HIGH, i);
		tp.submit(new Counter2(i3), Priority.HIGH, i);
		
		tp.runThreads();
		
		//wait until one of medium end
		try {
			f13.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		
		tp.shutDownNow();
		
		Integer ans = 0;
		while(Counter2.q.getSize() > 0) {
			try {
				ans += ((Integer) Counter2.q.dequeue()).intValue() % 10;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		assertEquals(ans.intValue() > 9, true);
		assertEquals(ans.intValue() < 15, true);
		
		int numTasks = tp.getNumOfTasks();
		
		tp.pause();
		
		assertEquals(numTasks, tp.getNumOfTasks());
	}
	
	@Test
	void TestAwaitTermination() {
		
		ThreadPool tp = new ThreadPool(4);
		
		Integer i = 210000001;
		Integer i2 = 21000002;
		Integer i3 = 210000003;
		Integer i4 = 21000004;
		
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		
		Future<Integer> f13 = tp.submit(new Counter2(i), Priority.MEDIUM, i);
		
		tp.submit(new Counter2(i), Priority.HIGH, i);
		tp.submit(new Counter2(i), Priority.HIGH, i);
		
		tp.runThreads();
		
		tp.awaitTermination();
		
		Integer ans = 0;
		while(Counter2.q.getSize() > 0) {
			try {
				ans += ((Integer) Counter2.q.dequeue()).intValue() % 10;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		assertEquals(ans.intValue(), 8);
		
		int numTasks = tp.getNumOfTasks();
		
		tp.pause();
		
		assertEquals(numTasks, tp.getNumOfTasks());
	}
	
	@Test
	void TestSetNumOfThreads() {
		
		ThreadPool tp = new ThreadPool(4);
		
		assertEquals(tp.getNumOfRunningThreads(), 4);
		
		tp.setNumOfThreads(3);
		
		assertEquals(tp.getNumOfRunningThreads(), 3);
		
		Integer i = 210000001;
		Integer i2 = 21000002;
		Integer i3 = 210000003;
		Integer i4 = 21000004;
		
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		
		Future<Integer> f13 = tp.submit(new Counter2(i), Priority.MEDIUM, i);
		
		tp.submit(new Counter2(i), Priority.HIGH, i);
		tp.submit(new Counter2(i), Priority.HIGH, i);

		tp.runThreads();
		
		assertEquals(tp.getNumOfRunningThreads(), 3);
		
		tp.setNumOfThreads(5);
		//assertEquals(tp.getNumOfRunningThreads(), 5);
		
		tp.awaitTermination();
		
//		try {
//			Thread.sleep(500);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		
		assertEquals(tp.getNumOfRunningThreads(), 0);
		
		tp = new ThreadPool(50);
		
		assertEquals(tp.getNumOfRunningThreads(), 50);
		
		tp.setNumOfThreads(53);
		
		assertEquals(tp.getNumOfRunningThreads(), 53);
		
		i = 210000001;
		i2 = 21000002;
		i3 = 210000003;
		i4 = 21000004;
		
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		
		f13 = tp.submit(new Counter2(i), Priority.MEDIUM, i);
		
		tp.submit(new Counter2(i), Priority.HIGH, i);
		tp.submit(new Counter2(i), Priority.HIGH, i);
		
		tp.runThreads();
		
		assertEquals(tp.getNumOfRunningThreads(), 53);
		
		tp.setNumOfThreads(48);
		//assertEquals(tp.getNumOfRunningThreads(), 48);
		
		tp.shutDownNow();
		
		assertEquals(tp.getNumOfRunningThreads() > 0, true);
	}
	
	@Test
	void TestCreateShut() {
		
		ThreadPool tp = new ThreadPool(4);
		Integer i = 210000001;
		Integer i2 = 21000002;
		Integer i3 = 210000003;
		Integer i4 = 21000004;
		
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		tp.submit(new Counter2(i), Priority.LOW, i);
		
		tp.shutDownNow();
		
		assertEquals(0, tp.getNumOfRunningThreads());

	}
	
}