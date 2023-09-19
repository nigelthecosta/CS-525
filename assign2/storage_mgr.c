#include "stdlib.h"
#include "string.h"
#include "storage_mgr.h"
#include "dberror.h"



void initStorageManager (void)
{
    printf("This is a storage manager program\n");
}


RC createPageFile (char *fileName)
{
	FILE *filePointer;		//file pointer


	filePointer = fopen(fileName, "w");	//Open the filePage in write mode

    if (filePointer==NULL) //checking to see if file exists
    {
        return RC_FILE_NOT_FOUND;
    }

    else
    {

        //Creating a new empty page in memory
        SM_PageHandle newPage = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char)); 


        // Writing the page to file.
		if(fwrite(newPage, sizeof(char), PAGE_SIZE,filePointer) < PAGE_SIZE)
			printf("write has failed \n");
		else
			printf("write is successful! \n");
            
		
		// Closing the file 
		fclose(filePointer);
		
		// De-allocating the memory  allocated to newPage to prevent memory leak
		free(newPage);
		
		return RC_OK;
    }

}


RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
	FILE *fptr;		//file pointer

	fptr = fopen(fileName, "r+");	//open the pageFile

	if(fptr!=NULL)	//if file exists
	{
		/*update the fileHandle attributes*/

		fHandle->fileName = fileName;	//store the file name

		/*read headerPage to get the Total Number of Pages*/
		char* readHeader;
		readHeader = (char*)calloc(PAGE_SIZE,sizeof(char));
		fgets(readHeader,PAGE_SIZE,fptr);
		char* totalPage;
		totalPage = readHeader;

		fHandle->totalNumPages = atoi(totalPage); //convert to integer
		fHandle->curPagePos = 0;	//store the current page position

		//store the File pointer information in the Management info of Page Handle
		fHandle->mgmtInfo = fptr;

		free(readHeader);		//free memory to avoid memory leaks

		return RC_OK;
	}
	else	//if file does not exists
	{
		return RC_FILE_NOT_FOUND;
	}
}


//Closes a file
RC closePageFile (SM_FileHandle *fHandle)
{
    //Checking if a file is found and if yes, file is closed
    if(fclose(fHandle->mgmtInfo)==0) 
    {
        return RC_OK;
    }
    else
    {
    return RC_FILE_NOT_FOUND;
    }
} 

//Destroys a page file
RC destroyPageFile (char *fileName)
{

    //Checking if a file is found and if yes, file is destroyed
    if(remove(fileName)==0)
    {
        return RC_OK;
    }
    else
    {

    return RC_FILE_NOT_FOUND;
    }
}


/* ----- READ FUNCTIONS -----*/

//Used to read a block of data from a file
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //check for a nonexistent page
    if (pageNum<0||pageNum>fHandle->totalNumPages ) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    	//If seeking the file pointer is successful then read the page into memPage

    fseek(fHandle->mgmtInfo, sizeof(char) * PAGE_SIZE * (pageNum+1), SEEK_SET);

    fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);

    //Update the fileHandle
	fHandle->curPagePos = pageNum;


    return RC_OK;
} 




//Return the current page position in a file
int getBlockPos (SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}


//Read the first Page of the file to memPage
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if(readBlock(0,fHandle,memPage)==RC_OK)
    {
		return RC_OK;
    }
	else
    {
		return RC_READ_NON_EXISTING_PAGE;
    }
}


//read the previous Page in a File
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(readBlock(getBlockPos(fHandle)-1,fHandle,memPage)== RC_OK)
    {
		return RC_OK;
    }
	else
    {
		return RC_READ_NON_EXISTING_PAGE;
    }
}

//read current Page in a file
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(readBlock(getBlockPos(fHandle),fHandle,memPage) == RC_OK)
    {
		return RC_OK;
    }
	else
    {
		return RC_READ_NON_EXISTING_PAGE;
    }
}

//read next Page in a file
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(readBlock(getBlockPos(fHandle),fHandle,memPage) == RC_OK)
    {
		return RC_OK;
    }    
	else
    {
		return RC_READ_NON_EXISTING_PAGE;
    }
}

//read the last Page in a file
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(readBlock(fHandle->totalNumPages - 1,fHandle,memPage)== RC_OK)
    {
		return RC_OK;
    }
	else
    {
		return RC_READ_NON_EXISTING_PAGE;
    }
}



/* ------ WRITE FUNCTIONS ------*/



//Write a data into a page specified by pageNum
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if(fHandle->totalNumPages<pageNum||pageNum<0)
        {
            return RC_WRITE_FAILED;
        }
    else
    {
		fseek(fHandle->mgmtInfo,(pageNum+1)*PAGE_SIZE,SEEK_SET);

		fwrite(memPage,PAGE_SIZE,1,fHandle->mgmtInfo);

		fHandle->curPagePos = pageNum;

		return RC_OK;

    }
}




//Write data into page currently pointed at by the file pointer
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//write on the current block
	if(RC_OK == writeBlock(getBlockPos(fHandle),fHandle,memPage))
		return RC_OK;
	else
		return RC_WRITE_FAILED;
}



//Append empty page onto the file
RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	//allocate a new empty page
	char * newPage;
	newPage = (char*)calloc(PAGE_SIZE, sizeof(char));

	//seek to the page
	fseek(fHandle->mgmtInfo,(fHandle->totalNumPages+1)*PAGE_SIZE,SEEK_SET);

	//if write is possible, then write the empty block onto the pageFile
	if(fwrite(newPage, PAGE_SIZE, 1, fHandle->mgmtInfo))
	{
		//update the fileHandle attributes

		fHandle->totalNumPages +=1;		//increment total number of pages
		fHandle->curPagePos = fHandle->totalNumPages - 1;	//update the current page position

		/*write the new total number of pages into the headerPage*/

		//seek to the start of the headerPage
		fseek(fHandle->mgmtInfo,0L,SEEK_SET);
		//write in file with formatting, the total number of pages
		fprintf(fHandle->mgmtInfo, "%d", fHandle->totalNumPages);

		//seek the pointer back again where it was last pointing
		fseek(fHandle->mgmtInfo,(fHandle->totalNumPages+1)*PAGE_SIZE,SEEK_SET);

		//free the memory allocated to avoid memory leaks
		free(newPage);

		return RC_OK;
	}
	else
	{
		free(newPage);
		return RC_WRITE_FAILED;
	}
}


RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	//Check if the capacity is already maintained
	if(fHandle->totalNumPages >= numberOfPages)
	{
		return RC_OK;
	}
	else	//if capacity less
	{
		int i, numOfPagesToAdd;
		//calculate the number of pages to be appended
		numOfPagesToAdd = numberOfPages - fHandle->totalNumPages;

		//loop through and append empty blocks to attain the required capacity
		for(i=0; i < numOfPagesToAdd ; i++)
		{
			//call to appendEmptyBlock()
			appendEmptyBlock(fHandle);
		}
		return RC_OK;
	}
}