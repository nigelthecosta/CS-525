Group 29 - Assignment 1 - Storage Manager 


Team Members: - Group 29
-----------------------------------------------------------
Nigel D'Costa
Vaishnavi Kadam
Rajnika Debnath
Nirbhay Rajgor
Zeal Patel

Nigel D'Costa - Implemented all the read functions and a few test cases.
Vaishnavi Kadam - Implemented write functions
Rajnika Debnath - Implemented the file creation and handling methods 
Nirbhay Rajgor - Implemented the append block and ensure capacity methods
Zeal Patel - Implemented make file and a few test cases.

Apart from this, all members actively took part in testing and debugging the code.

Description
---------------------------------------------------------

Implementation of a simple storage manager - a module that is capable of reading blocks
from a file on disk into memory and writing blocks from memory to a file on disk.


How to run
-----------------------------------------------------------

1. Open terminal and Clone from BitBucket to the required location.

2. Navigate to assign1 folder.

3. Use make command to execute the Makefile, type make
4. 



Solution Description
-----------------------------------------------------------

createPageFile -->In this method 
creation of Page File is done and also allocate '\0' bytes to the firstPage page. Also headerPage page is reserved at the start of the page file to store additional file information like total number of pages in it.


openPageFile -->
 It Opens an existing page file specified by fileName. Used to store page file information/ attributes like curPagePos, totalNumPages and mgmtInfo. The totalNumPage information is set from the headerPage of the page file. Throws RC_FILE_NOT_FOUND error if page file not found to be opened. 

closePageFile & destroyPageFile -->
 Closes an open page file or destroy (delete) a page file. Throws RC_FILE_NOT_FOUND error if page file not found to be opened.


readBlock -->
 The method reads the pageNumth block from a file and stores its content in the memory pointed to the memPage page handle. Saves/ Updates the curPagePos information of the page file. If the file has less than pageNum pages, the method returns RC_READ_NON_EXISTING_PAGE.


getBlockPos -->
 Used to return the current page position of the file, that is available in fileHandle attribute 'curPagePos'.


readFirstBlock, readLastBlock -->
 Reads the first and last page of a file by sending the pageNumth information while calling the readBlock method.


readPreviousBlock, readCurrentBlock, readNextBlock --> 
Reads the current, previous, or next page relative to the curPagePos of the file by sending the pageNumth information while calling the readBlock method. The curPagePos is moved to the page that was read. If the user tries to read a block before the first page or after the last page of the file, the method returns RC_READ_NON_EXISTING_PAGE error.


writeBlock, writeCurrentBlock -->
 Writes a page to disk using either the current position or an absolute position. For writeBlock() method if absolute position is out of range then it returns RC_WRITE_FAILED error and for failure of write operations on any page of file it returns RC_WRITE_FAILED error.


appendEmptyBlock --> 
The pagefile is appended by one page (Empty Block). The value of the totalNumPage along with the headerPage page information is also incremented by one. The new last page is filled with zero bytes. The curPagePos is set to the position of the appended page in the file. Returns RC_WRITE_FAILED error if write operation fails on the page of the file.


ensureCapacity --> 
The size of the file is increased to numberOfPages by calling appendEmptyBlock method to add the remaining empty pages to the existing pages in the file.



Test Cases
-----------------------------------------------------------
The program verifies all the test cases that are mentioned in the test file i.e test_assign1_1 and ensures that there are no errors. Along with the default test case given, there is an additional test case prepared i.e test_assign1_2 which tests all the methods that have been implemented, and runs successfully. Furthermore, in the implementation design we have taken steps to avoid memory leaks.

To Run the additional Test Case
-----------------------------------------------------------
In terminal,
1. Type
	make test_case2
2. To clean, type
	make clean


