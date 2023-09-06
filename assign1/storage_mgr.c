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

        //Creating a new empty page in the memory
        SM_PageHandle newPage = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char)); 


        // Writing the page to file.
		if(fwrite(newPage, sizeof(char), PAGE_SIZE,filePointer) < PAGE_SIZE)
			printf("write failed \n");
		else
			printf("write succeeded \n");
            
		
		// Closing the file 
		fclose(filePointer);
		
		// De-allocating the memory  allocated to newPage to prevent memory leak
		free(newPage);
		
		return RC_OK;
    }

}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    FILE *filePointer;
    filePointer = fopen(fileName,"r"); //opening file in read mode

    //Check to see if the file exists
    if (filePointer==NULL)
    {
        return RC_FILE_NOT_FOUND;
    }



    else
    {
        fHandle->fileName = fileName;
        fHandle->curPagePos = 0;
        fseek(filePointer, 0, SEEK_END); //move file pointer to end of file
        long length = ftell(filePointer); //Get the length of the file. ftell() returns the  current file position.
        int totalPages = (int)length/PAGE_SIZE; //calculating the total number of pages in the file

        //Setting the file handle attributes
        
        fHandle->totalNumPages = totalPages;
        
        fHandle->mgmtInfo = filePointer;
        return RC_OK;
    }
} 

/*Closes a file that is open given a file handle */
RC closePageFile (SM_FileHandle *fHandle)
{
    //Close file as well as checking that it was found
    if(fclose(fHandle->mgmtInfo)==0) //close file
    {
        return RC_OK;
    }
    else
    {
    return RC_FILE_NOT_FOUND;
    }
} // end closePageFile

/*Destroys a file given a file name*/
RC destroyPageFile (char *fileName)
{

    //Destroy the file as well as checking that it was found
    if(remove(fileName)==0)
    {
        return RC_OK;
    }
    else
    {

    return RC_FILE_NOT_FOUND;
    }
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //check for a nonexistent page
    if (fHandle->totalNumPages < pageNum) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    	//if seeking the file ptr is successful then read the page into memPage

    fseek(fHandle->mgmtInfo, sizeof(char) * PAGE_SIZE * (pageNum+1), SEEK_SET);

    fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);

    //Update the fileHandle
    fHandle->curPagePos = pageNum;

    return RC_OK;
} //end readBlock






int getBlockPos (SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}

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


RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(readBlock(getBlockPos(fHandle)+1,fHandle,memPage) == RC_OK)
    {
		return RC_OK;
    }    
	else
    {
		return RC_READ_NON_EXISTING_PAGE;
    }
}


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

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//write on the current block
	if(RC_OK == writeBlock(getBlockPos(fHandle),fHandle,memPage))
		return RC_OK;
	else
		return RC_WRITE_FAILED;
}


extern RC appendEmptyBlock (SM_FileHandle *fHandle)
{
  
	SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
	
	
	int isSeekSuccess = fseek(pageFile, 0, SEEK_END);
	
	if( isSeekSuccess == 0 ) {
		// Writing an empty page to the file
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, pageFile);
	} else {
		free(emptyBlock);
		return RC_WRITE_FAILED;
	}
	
	
	free(emptyBlock);
	
	fHandle->totalNumPages++;
	return RC_OK;
}



extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{

   FILE *fpointer;

   fpointer = fHandle->mgmtInfo;

   if (fHandle->totalNumPages>numberOfPages) 
   {
      fHandle->totalNumPages = numberOfPages;
      appendEmptyBlock(fHandle);
      printf("Appending empty block....\n");

    }

  return RC_OK;
}
