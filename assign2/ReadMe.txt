Group 29 - Assignment 2 - Buffer Manager

Team Members: - Group 29
---------------------------------------------------------
Nigel D'Costa
Vaishnavi Kadam
Rajanika Debnath
Nirbhay Rajgor
Zeal Patel

Nigel D'Costa - Implemented functions for FIFO,initBufferPool,shutdownBufferPool,forceFlushPool.
Vaishnavi Kadam - Implemented functions for LRU, markDirty, unpinPage, 
Rajanika Debnath - Implemented functions for forcePage, pinPage, buffPoolCreate (Custom function)
Nirbhay Rajgor - Implemented functions for getFrameContents,getDirtyFlags
Zeal Patel - Implemented functions for getFixCounts,getNumReadIO,getNumWriteIO

Apart from this, all members actively took part in testing and debugging the code.




Description

---------------------------------------------------------


Implement a buffer manager that manages a buffer of blocks in memory including reading/flushing to disk and block replacement.





How to run

-----------------------------------------------------------


1. Open terminal 

2. Navigate to assign2 folder.

3. Use make command to execute FIFO and LRU page replacement strategies,
 
	$ make

4. To clean,
	$ make clean






Solution Description

-----------------------------------------------------------



Buffer Pool Functions

---------------------


initBufferPool():
Creates a new buffer pool with numPages page frames using the page replacement strategy. The pool is used to cache pages from the page file with name pageFileName.




shutdownBufferPool():
Destroys a buffer pool and frees all the memory allocated for page frames.



forceFlushPool():
Causes all dirty pages (with fix count 0) from the buffer pool to be written to pageFile.




Page Management Functions

-------------------------


pinPage():

calls the following functions using an if else:

1. FIFO - it implements circular queue for page replacement using First In First Out strategy.

2. LRU - it implements circular queue for page replacement using Least Recently Used strategy.


The working of pinPage is as follows:

1. calls the openPageFile() to open the page file.

2. if occupiedCount is less than buffer pool size then pin the page in the pool, else use one of the page replacement strategy.

3. If a page needs to be replaced,first check if the page is dirty and call writeBlock() to write the page to the page file

4. calls readBlock() pins a page identified by pageNum in the buffer pool.

5. countSet is incremented to show that the page is in use

6. Capacity of the buffer pool is changed accordingly

7. Page file is closed using closePageFile()




unpinPage():
unpins the page using the pageNum field


markDirty():
sets a page as dirty identified by pageNum of the frame in the buffer pool.


forcePage():
calls writeBlock() to write the page back to the disk


Statistics Functions

--------------------


getFrameContents():
Returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in  the ith page frame. An empty page frame is represented using the constant NO_PAGE.



getDirtyFlags():
Returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty. Empty page frames are considered as clean.



getFixCounts():
Returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame. Return 0 for empty page frames.



getNumReadIO():
Returns the number of pages that have been read from disk since a buffer pool has been initialized. 


getNumWriteIO():
Returns the number of pages written to the page file since the buffer pool has been initialized.





Test Cases

-----------------------------------------------------------

Files: test_assign2_1.c(FIFO & LRU)

 The program verifies all the test cases that are mentioned in the test file i.e test_assign2_1 and ensures that there are no errors.

