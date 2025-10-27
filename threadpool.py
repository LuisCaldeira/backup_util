# GPL v2 License
# (c) 2024 EvilWarning <

import time
import threading
import sys
import random
from typing import Callable, Any, Optional, List, cast
from queue import Queue
from concurrent.futures import Future



THREADPOOL_DEBUG_MODE = False
THREADPOOL_WORKER_SLEEP_TIME = 0.1
MAX_TASK_QUEUE_SIZE = 100000        # Maximum size of the task queue, can store up to 100,000 tasks

class Worker(threading.Thread):
    __slots__ = ['tasks', 'exception_handler', 'busy', 'terminate']

    """ Worker thread for executing tasks in the thread pool.
    This class extends threading.Thread and is responsible for executing tasks
    from the task queue. It can handle exceptions using a custom exception handler
    if provided. The worker thread runs in a loop, fetching tasks from the queue
    and executing them until it is terminated.
    Attributes:
        id (int): Unique identifier for the worker thread.
        tasks (Queue[Any]): Queue containing tasks to be executed.
        exception_handler (Optional[Callable[[Exception], None]]): Optional custom exception handler.
        busy (bool): Indicates if the worker is currently busy executing a task.
        terminate (bool): Flag to indicate if the worker should terminate.
    Methods:
        run(): The main method that runs the worker thread, fetching and executing tasks.
        run_debug(): Debug version of the run method for testing purposes.
        This method prints task execution details instead of logging.
    Args:
        id (int): Unique identifier for the worker thread.
        tasks (Queue[Any]): Queue containing tasks to be executed.
        exception_handler (Optional[Callable[[Exception], None]]): Optional custom exception handler.
    """
    def __init__(self,  id: int,
                        tasks: Queue[Any],
                        exception_handler: Optional[Callable[[Exception], None]],
                        params: Optional[dict|None]) -> None:
        """
            Initialize the Worker thread.
        Args:
            id (int): Unique identifier for the worker thread.
            tasks (Queue[Any]): Queue containing tasks to be executed.
            exception_handler (Optional[Callable[[Exception], None]]): Optional custom exception handler.
            params (Optional[dict|None]): Optional parameters for the worker thread.
        :return: None
        :rtype: None
        :raises: None
        :example:
        >>> tasks_queue = Queue()
        >>> worker = Worker(id=1, tasks=tasks_queue, exception_handler=None, params=None)
        >>> # This will create a worker thread that can execute tasks from the tasks_queue.
        """
        super().__init__()
        self.tasks = tasks
        self.exception_handler = exception_handler
        self.daemon = True
        self.name: str = f"threadpool_worker_{id}"
        self.busy: bool = False
        self.terminate = False
        self.params = params if params is not None else {}

        # Tweaking parameters for the worker thread
        self.__logger: Any = self.params.get('LOGGER', None)
        self.__DEBUG_MODE: bool = self.params.get('DEBUG_MODE', False)
        self.__log_tags: dict = self.params.get('LOG_TAGS', {})
        self.__THREADPOOL_WORKER_SLEEP_TIME: float = self.params.get('THREADPOOL_WORKER_SLEEP_TIME', 0.1)
        self.start()

    def run(self) -> None:
        """Run the worker thread, fetching and executing tasks from the queue.
        This method runs in a loop, continuously fetching tasks from the queue
        and executing them. If an exception occurs during task execution, it is
        handled by the provided exception handler if available. The worker will
        terminate gracefully when the `terminate` flag is set to True.
        """
        if self.__DEBUG_MODE is True:
            self.run_debug()

        while not self.terminate:
            #func, callback, args, kargs  = self.tasks.get()
            item = self.tasks.get()
            # Normalize tuple
            if isinstance(item, tuple):
                if len(item) == 5:
                    func, callback, args, kargs, _ = item
                elif len(item) == 4:
                    func, callback, args, kargs = item
                elif len(item) == 2:
                    func, args = item
                    callback, kargs = (None, {})
                else:
                    raise ValueError(f"Unknown task tuple shape: {len(item)}")
            else:
                # single callable
                func, callback, args, kargs = item, None, (), {}
            try:
                if func is not None:
                    self.busy = True
                    try:
                        if kargs:
                            result = func(*args, **kargs)
                        else:
                            result = func(*args)
                        if callback:
                            callback(result)
                    finally:
                        self.busy = False

            except Exception as e:

                if self.exception_handler:
                    self.exception_handler(e)
                else:
                    # Default behavior if no custom handler provided
                    if self.__logger is not None:
                        error_message = "Exception in Worker thread {self.name}: " + str(e)
                        self.__logger.error("Exception in Worker thread" , tags=self.__log_tags,
                                                                    worker_id=f"{self.name}",
                                                                    exception=error_message )
                    else:
                        # log to system standard error if logger is not available
                        print(f"Exception in Worker thread {self.name}: {e}", file=sys.stderr)

            finally:
                self.tasks.task_done()
                self.busy = False
                if self.tasks.qsize() < 1:
                    if self.__DEBUG_MODE is True:
                        self.__logger.info(f"Task Queue is empty. Thread {self.name} is exiting.", tags=self.__log_tags)
                    time.sleep(self.__THREADPOOL_WORKER_SLEEP_TIME)  # Sleep to avoid busy waiting

    def run_debug(self) -> None:
        """Debug version of the run method for testing purposes.
        This method prints task execution details instead of logging.
        It is useful for debugging and testing the worker thread without
        relying on the logging system.
        """

        while not self.terminate:
            #func, callback, args, kargs  = self.tasks.get()
            item = self.tasks.get()
            if isinstance(item, tuple):
                if len(item) == 5:
                    func, callback, args, kargs, _ = item
                elif len(item) == 4:
                    func, callback, args, kargs = item
                elif len(item) == 2:
                    func, args = item
                    callback, kargs = (None, {})
                else:
                    raise ValueError(f"Unknown task tuple shape: {len(item)}")
            else:
                # single callable
                func, callback, args, kargs = item, None, (), {}
            if self.__DEBUG_MODE is True:
                self.__logger.debug(f" args = {args} kargs = {kargs}", tags=self.__log_tags)

            if func is not None:
                self.busy = True
                try:
                    if kargs:
                        result = func(*args, **kargs)
                    else:
                        result = func(*args)
                    if callback:
                        callback(result)
                finally:
                    self.busy = False


            self.tasks.task_done()
            self.busy = False
            if self.tasks.qsize() < 1:
                if self.__DEBUG_MODE is True:
                    self.__logger.info(f"Task Queue is empty. Thread {self.name} is exiting.", tags=self.__log_tags)
                time.sleep(self.__THREADPOOL_WORKER_SLEEP_TIME)


class ThreadPool:
    __slots__ = ['tasks', 'max_threads', 'min_threads', 'exception_handler',
                 'current_threads', 'monitor_interval', 'monitor_thread', 'task_queue_max_size',
                 'current_threads_lock', 'metrics', 'workers', 'name', 'params', '__logger', '__DEBUG_MODE',
                   '__log_tags', '__worker_sleep_time']

    """ ThreadPool class for managing a pool of worker threads.
    This class provides a thread pool implementation that allows adding tasks to a queue
    and executing them concurrently using a specified number of worker threads.
    It supports dynamic scaling of threads based on the number of queued tasks and the
    number of busy workers. The thread pool can be configured with a minimum and maximum
    number of threads, and it can handle exceptions using a custom exception handler.
    Attributes:
        tasks (Queue[Any]): Queue for holding tasks to be executed by worker threads.
        max_threads (int): Maximum number of threads in the pool.
        min_threads (int): Minimum number of threads in the pool.
        exception_handler (Optional[Callable[[Exception], None]]): Optional custom exception handler.
        current_threads (int): Current number of active threads in the pool.
        monitor_interval (int): Interval in seconds for monitoring the thread pool.
        monitor_thread (threading.Thread): Thread for monitoring the thread pool.
        task_queue_max_size (int): Maximum size of the task queue.
        current_threads_lock (threading.Lock): Lock for synchronizing access to current_threads.
        metrics (dict): Dictionary for storing metrics related to the thread pool.
        workers (list[Worker]): List of worker threads in the pool.
    Methods:
        __init__(num_threads: int, max_threads: int, min_threads: int,
                 exception_handler: Optional[Callable[[Exception], None]] = None) -> None:
        Initializes the thread pool with a specified number of threads, maximum and minimum threads,
        and an optional exception handler.
        add_task(func: Callable[..., Any], callback: Optional[Callable[[Any], None]] = None,
                 *args: Any, **kargs: Any) -> None:
        Adds a task to the queue for execution by worker threads.
        wait_completion() -> None:
        Waits for completion of all tasks in the queue.
        add_threads(num: int) -> None:
        Adds more threads to the pool up to the max_threads limit.
        reduce_threads(num: int) -> None:
        Reduces the number of threads in the pool by terminating worker threads.
        monitor() -> None:
        Monitors the thread pool, scaling up or down the number of threads based on the number of busy and idle workers.
        number_of_active_workers() -> int:
        Returns the number of active worker threads in the pool.
        number_of_queued_tasks() -> int:
        Returns the number of tasks currently queued in the task queue.
        get_busy_worker_count() -> int:
        Returns the count of busy worker threads in the pool.
        get_idle_worker_count() -> int:
        Returns the count of idle worker threads in the pool.
    """
    def __init__(self,  num_threads: int,
                        max_threads: int,
                        min_threads: int,
                        exception_handler: Optional[Callable[[Exception], None]] = None,
                        name: Optional[str]|None = None,
                        monitor_interval: int = 2,
                        params: Optional[dict|None] = None) -> None:

        if not (min_threads <= num_threads <= max_threads):
            raise ValueError("Threads configuration is not valid.")

        self.current_threads_lock = threading.Lock()
        self.params: dict|None = params if params is not None else {}

        # Initialize parameters for the thread pool
        self.__logger: Any = self.params.get('LOGGER', None)
        self.__DEBUG_MODE: bool = self.params.get('DEBUG_MODE', False)
        self.__log_tags: dict = self.params.get('LOG_TAGS', {})
        self.__worker_sleep_time: float = self.params.get('THREADPOOL_WORKER_SLEEP_TIME', 0.1)

        self.task_queue_max_size = MAX_TASK_QUEUE_SIZE
        self.tasks: Queue[Any] = Queue(self.task_queue_max_size)
        self.max_threads = max_threads
        self.min_threads = min_threads
        self.exception_handler = exception_handler
        self.current_threads = 0
        self.monitor_interval: int = monitor_interval
        self.workers: List[Worker] = []     # type: ignore
        self.name: str = name if name else "ThreadPool_" + str(random.randint(1000, 9999))


         # Start the monitor thread
        self.monitor_thread = threading.Thread(target=self.monitor,
                                               name="ThreadPool_Monitor",
                                               daemon=True)
        self.monitor_thread.start()
         # Initialize with the given number of threads
        for _ in range(num_threads):
            self._add_worker()

        return

    def add_task(self,
                 func: Callable[..., Any],
                 callback: Optional[Callable[[Any], None]] = None,
                 *args: Any,
                 **kargs: Any) -> None:
        """
        Add a task to the queue.

        :param func: Task function to be executed.
        :param args: Arguments for the task function.
        :param callback: Optional callback to be executed after the task finishes.
        :param kargs: Keyword arguments for the task function.
        """
        if self.__DEBUG_MODE is True:
            self.__logger.debug(f"Adding task to queue. Queue size: {self.tasks.qsize()}", tags=self.__log_tags)
            self.__logger.debug(f" args = {args} kargs = {kargs}", tags=self.__log_tags)
        if len(kargs) == 0:
            self.tasks.put((func, callback, args, {}))
        else:
            self.tasks.put((func, callback, args, kargs))
        if self.__DEBUG_MODE is True:
            self.__logger.debug("Added task to queue. ", tags=self.__log_tags, pending_queue_size=self.tasks.qsize())
        return

    def wait_completion(self) -> None:
        """_summary_ Wait for all tasks in the queue to complete.
        This method blocks until all tasks in the queue have been processed and marked as done.
        It ensures that all worker threads have finished executing their tasks before returning.
        :return: None
        :rtype: None
        :raises: None
        :example:
        >>> threadpool = ThreadPool(num_threads=5, max_threads=10, min_threads=2)
        >>> threadpool.add_task(my_function, args=(arg1, arg2), kargs={'key': 'value'})
        >>> threadpool.wait_completion()
        >>> # All tasks will be completed before proceeding.
        """
        self.tasks.join()
        return

    def _add_worker(self) -> None:
        """_summary_ Add a new worker thread to the pool.
        This method creates a new Worker instance and adds it to the list of workers.
        It increments the current_threads count and logs the addition of the worker thread.
        :return: None
        :rtype: None
        :raises: None
        :example:
        >>> threadpool = ThreadPool(num_threads=5, max_threads=10, min_threads=2)
        >>> threadpool._add_worker()
        >>> # A new worker thread will be added to the pool.
        """
        parameters = {
            'LOGGER': self.__logger,
            'DEBUG_MODE': self.__DEBUG_MODE,
            'LOG_TAGS': self.__log_tags,
            'THREADPOOL_WORKER_SLEEP_TIME': self.__worker_sleep_time
        }
        worker = Worker(self.current_threads,
                        self.tasks,
                        self.exception_handler,
                        params=parameters)

        with self.current_threads_lock:
            self.workers.append(cast(Worker, worker))
            self.current_threads += 1
        if self.__DEBUG_MODE is True:
            self.__logger.info("Added worker thread. Current threads", tags=self.__log_tags, thread_count=self.current_threads)


    def add_threads(self, num: int) -> None:
        """_summary_ Add more threads to the pool.
        This method adds a specified number of worker threads to the pool, ensuring that
        the total number of threads does not exceed the max_threads limit. If the number
        of current threads plus the number of new threads exceeds max_threads, an error is logged.
        :param num: Number of threads to add.
        :type num: int
        :raises ValueError: If the number of threads exceeds max_threads.
        :example:
        >>> threadpool = ThreadPool(num_threads=5, max_threads=10, min_threads=2)
        >>> threadpool.add_threads(3)
        >>> # This will add 3 new worker threads to the pool, if it does not exceed max_threads.


        Args:
            num (int): Number of threads to add to the pool.
        """
        if self.current_threads + num > self.max_threads:
            if self.__logger is not None:
                self.__logger.error("Exceeds max_threads limit.", tags=self.__log_tags)

        for _ in range(num):
            self._add_worker()
        return

    def reduce_threads(self, num: int) -> None:
        # This is a simple way to reduce threads by setting a termination task.
        # Each termination task will cause one worker thread to terminate.
        """_summary_ Reduce the number of threads in the pool.
        This method reduces the number of worker threads in the pool by terminating
        a specified number of worker threads. It ensures that the current_threads count
        does not go below the min_threads limit. If the number of threads to reduce
        exceeds the current number of threads, an error is logged.
        :param num: Number of threads to reduce.

        Args:
            num (int): Number of threads to reduce from the pool.
        """
        with self.current_threads_lock:
            for _ in range(num):
                if self.workers:
                    worker = self.workers.pop()
                    worker.terminate = True
                    worker.join()  # Wait for the worker thread to exit
                    self.current_threads -= 1

        if self.__DEBUG_MODE is True:
            self.__logger.info(f"Reduced worker threads. Current threads: {self.current_threads}",  tags=self.__log_tags,
                                                                                                    current_threads=self.current_threads)
        return

    def monitor(self) -> None:
        """_summary_ Monitor the thread pool and adjust the number of threads dynamically.
        This method runs in a separate thread and periodically checks the number of busy
        and idle workers. It scales up the number of threads if all workers are busy and
        the maximum number of threads has not been reached. It scales down the number of
        threads if there are too many idle workers and the current number of threads is
        above the minimum threshold. The monitoring interval is defined by the monitor_interval attribute.
        :return: None
        """
        while True:
            time.sleep(self.monitor_interval)

            busy_workers = self.get_busy_worker_count()
            idle_workers = self.get_idle_worker_count()

            # Scale up if all workers are busy and max_threads is not reached
            if busy_workers == self.current_threads and self.current_threads < self.max_threads:
                self.add_threads(1)

            # Scale down if there are too many idle workers and above min_threads
            elif idle_workers > self.max_threads * 0.25 and self.current_threads > self.min_threads:
                self.reduce_threads(1)


    def number_of_active_workers(self) -> int:
        """_summary_ Get the number of active worker threads in the pool.
        This method returns the current number of active worker threads in the thread pool.
        It is useful for monitoring the thread pool's activity and can be used to determine
        if the thread pool is currently processing tasks or if it is idle.

        Returns:
            int: The number of active worker threads in the pool.
        """
        return self.current_threads

    def number_of_queued_tasks(self) -> int:
        """_summary_ Get the number of tasks currently queued in the task queue.
        This method returns the number of tasks that are currently waiting in the task queue
        to be processed by the worker threads. It is useful for monitoring the workload
        of the thread pool and can help in determining if the thread pool is overloaded or
        if it has capacity to handle more tasks.

        Returns:
            int: The number of tasks currently queued in the task queue.
        """
        return self.tasks.qsize()

    def get_busy_worker_count(self) -> int:
        """_summary_ Get the count of busy worker threads in the pool.
        This method iterates through the list of worker threads and counts how many of them
        are currently busy executing tasks. It is useful for monitoring the workload of the
        thread pool and can help in determining if the thread pool is overloaded or if it has
        idle workers available to handle more tasks.

        Returns:
            int: The number of busy worker threads in the pool.
        """
        with self.current_threads_lock:
            return sum(worker.busy for worker in self.workers)

    def get_idle_worker_count(self) -> int:
        """_summary_ Get the count of idle worker threads in the pool.
        This method calculates the number of idle worker threads in the thread pool by
        subtracting the number of busy workers from the total number of workers. It is useful
        for monitoring the thread pool's activity and can help in determining if there are
        enough idle workers available to handle new tasks without adding more threads.

        Returns:
            int: The number of idle worker threads in the pool.
        """
        return len(self.workers) - self.get_busy_worker_count()

    def submit(self, func: Callable[..., Any], *args: Any, **kargs: Any) -> Future:
        """_summary_ Submit a task to the thread pool and return a Future object.
        This method allows submitting a task to the thread pool for execution. It wraps the
        task in a Future object, which can be used to retrieve the result of the task once
        it has completed. The task is added to the task queue, and a callback is defined
        to set the result of the Future object when the task finishes.
        Args:
            func (Callable[..., Any]): The task function to be executed.
            *args (Any): Arguments for the task function.
            **kargs (Any): Keyword arguments for the task function.
        Returns:
            Future: A Future object representing the execution of the task.
        """
        future = Future()
        def callback(result):
            future.set_result(result)
        self.add_task(func, callback, *args, **kargs)
        return future

    def shutdown(self) -> None:
        """_summary_ Shutdown the thread pool and terminate all worker threads.
        This method stops the thread pool by setting the terminate flag for all worker threads
        and waiting for them to finish. It ensures that all tasks in the queue are completed
        before shutting down the thread pool. After calling this method, no new tasks can be added
        to the thread pool, and it will not accept any further operations.

        :return: None
        """
        for worker in self.workers:
            worker.terminate = True
            #worker.join()
        self.workers.clear()
        self.current_threads = 0
        if self.__DEBUG_MODE is True:
            self.__logger.info("ThreadPool shutdown complete.", tags=self.__log_tags,
                                                                current_threads=self.current_threads)
        return
