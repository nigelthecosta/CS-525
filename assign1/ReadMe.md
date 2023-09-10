Group 29 - Assignment 1 - Storage Manager 


Team Members: - Group 29
-----------------------------------------------------------
Nigel D'Costa <br>
Vaishnavi Kadam <br>
Rajanika Debnath <br>
Nirbhay Rajgor <br>
Zeal Patel <br>

Nigel D'Costa - Implemented all the read functions and a few test cases.  
Vaishnavi Kadam - Implemented write functions <br>
Rajanika Debnath - Implemented the file creation and handling methods  <br>
Nirbhay Rajgor - Implemented the append block and ensure capacity methods <br>
Zeal Patel - Implemented make file and a few test cases. <br>

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

4. To run additional test cases, type make test_case2



Solution Description
-----------------------------------------------------------

createPageFile(): <br>
Ceates a new page file. Returns RC_FILE_NOT_FOUND if file could not be created 

openPageFile(): <br>
 It Opens an existing page file specified by fileName. Stores page file information such as curPagePos, totalNumPages and mgmtInfo.  Throws RC_FILE_NOT_FOUND error if page file is not found.

closePageFile(): <br>
Closes an open page file. Throws RC_FILE_NOT_FOUND error if page file not found.

destroyPageFile(): <br>
Deletes a page file. Throws RC_FILE_NOT_FOUND error if page file not found.


readBlock(): <br>
The method reads the block at position pageNum from a file and stores its content in the memory pointed
to by the memPage page handle.
If the file has less than pageNum pages, the method returns RC_READ_NON_EXISTING_PAGE.

getBlockPos(): <br>
Used to return the current page position of the file, that is available in fileHandle attribute 'curPagePos'.


readFirstBlock(): <br>
Reads the first  page of a file by specifying the pageNum as 0  while calling the readBlock method.


readPreviousBlock():  <br>
Reads the previous page relative to the curPagePos of the file by sending the pageNum-1th information while calling the readBlock method. The curPagePos is moved to the page that was read. If the page is not found, the method returns RC_READ_NON_EXISTING_PAGE error.

readCurrentBlock(): <br>
Reads the current page  of the file by sending the pageNumth information while calling the readBlock method. The curPagePos is moved to the page that was read. If the page is not found, the method returns RC_READ_NON_EXISTING_PAGE error.

readNextBlock(): <br>
Reads the next page relative to the curPagePos of the file by sending the pageNum+1th information while calling the readBlock method. The curPagePos is moved to the page that was read. If the page is not found, the method returns RC_READ_NON_EXISTING_PAGE error.

readLastBlock(): <br>
Reads the last  page of a file by specifying the pageNum as totalNumPages-1  while calling the readBlock method. If the page is not found, the method returns RC_READ_NON_EXISTING_PAGE error.


writeBlock(): <br>
Writes a page to disk using the position passed to pageNum. If position is out of range then it returns RC_WRITE_FAILED error.

 
writeCurrentBlock(): <br>
Writes a page to disk using  the current position of the file pointer. If position is out of range then it returns RC_WRITE_FAILED error.



appendEmptyBlock(): <br>
Appends empty page to the end of file. Writes '\0' bites into page.

ensureCapacity(): <br>
The size of the file is increased to numberOfPages by calling appendEmptyBlock method to add the remaining empty pages to the existing pages in the file.



Test Cases
-----------------------------------------------------------
The program verifies all the test cases that are mentioned in the test file i.e test_assign1_1 and ensures that there are no errors. Along with the default test case given, there is an additional test case prepared i.e test_assign1_2 which tests all the methods that have been implemented, and runs successfully. 


