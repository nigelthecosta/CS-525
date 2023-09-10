#include "stdlib.h"
#include "string.h"
#include "storage_mgr.h"
#include "dberror.h"



void initStorageManager (void)
{
    printf("This is a storage manager program\n");
}


//Creating a page file
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


//Opening a page file
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
        //Setting the name of the file handle
        fHandle->fileName = fileName;

        //Setting the current position to the beginning of the page
        fHandle->curPagePos = 0;

        //moving file pointer to end of file
        fseek(filePointer, 0, SEEK_END); 


         //Get the length of the file. ftell() returns the  current position of the file.
        long length = ftell(filePointer); 
        
        //Calculating the total number of pages in the file by divinding the length by the page size
        int totalPages = (int)length/PAGE_SIZE; 
        
        //Setting the file handle attributes
        fHandle->totalNumPages = totalPages;       
        fHandle->mgmtInfo = filePointer;
        return RC_OK;
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
extern RC appendEmptyBlock (SM_FileHandle *fHandle)
{
  FILE *filePointer;
  int i;
	filePointer = fopen(fHandle->fileName,"r+");


	//Move file pointer to end of file
	fseek(filePointer, 0, SEEK_END);


	//write into an empty page
	for(i = 0; i < PAGE_SIZE; i++)
	{
		fwrite("\0",1, 1,filePointer);
		fseek(filePointer,0,SEEK_END);
	}

	fHandle->mgmtInfo = filePointer;
	fHandle->totalNumPages = (ftell(filePointer)/PAGE_SIZE);
	return RC_OK;
}


/*Check if the page file has required number of pages, if not, add the pages required to get to that capacity*/
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{

   FILE *filePointer;

   filePointer = fHandle->mgmtInfo;

   if (fHandle->totalNumPages>numberOfPages) 
   {
      fHandle->totalNumPages = numberOfPages;
      appendEmptyBlock(fHandle);
      printf("Appending empty block....\n");

    }

  return RC_OK;
}
