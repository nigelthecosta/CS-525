#include "stdlib.h"
#include "string.h"
#include "storage_mgr.h"
#include "dberror.h"



void initStorageManager (void)
{
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
	FILE *filePointer;		//file pointer

	filePointer = fopen(fileName, "r+");	//open the pageFile

	if(filePointer!=NULL)	//checking to see if file exists
	{

		fHandle->fileName = fileName;	//store the file name

		/*read headerPage to get the Total Number of Pages*/
		char* r;
		r = (char*)calloc(PAGE_SIZE,sizeof(char));
		fgets(r,PAGE_SIZE,filePointer);
		char* page_total;
		page_total = r;

		fHandle->totalNumPages = page_total; 
		fHandle->curPagePos = 0;	//store the current position of the page

		fHandle->mgmtInfo = filePointer;

		free(r);		//avoiding memory leaks

		return RC_OK;
	}
	//if file does not exist
	else	
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
	char * additionalPage;
	additionalPage = (char*)calloc(PAGE_SIZE, sizeof(char));

	//seek to the page
	fseek(fHandle->mgmtInfo,(fHandle->totalNumPages+1)*PAGE_SIZE,SEEK_SET);

	//writing the new block into the page file
	if(fwrite(additionalPage, PAGE_SIZE, 1, fHandle->mgmtInfo))
	{

		fHandle->totalNumPages=fHandle->totalNumPages+1;		
		fHandle->curPagePos = fHandle->totalNumPages - 1;	//update the current page position


		//seek to the start of the fHandle
		fseek(fHandle->mgmtInfo,0L,SEEK_SET);
		fprintf(fHandle->mgmtInfo, "%d", fHandle->totalNumPages);

		//moving the pointer to last pointed position
		fseek(fHandle->mgmtInfo,(fHandle->totalNumPages+1)*PAGE_SIZE,SEEK_SET);

		//avoiding memory leaks
		free(additionalPage);

		return RC_OK;
	}
	else
	{
		free(additionalPage);
		return RC_WRITE_FAILED;
	}
}


RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	//Checking whether the capacity is available
	if(fHandle->totalNumPages >= numberOfPages)
	{
		return RC_OK;
	}
	else	
	{
		int i, pages_added;
		//Calculating how many pages need to be added
		pages_added = numberOfPages - fHandle->totalNumPages;

		for(i=0; i < pages_added ; i++)
		{
			appendEmptyBlock(fHandle);
		}
		return RC_OK;
	}
}