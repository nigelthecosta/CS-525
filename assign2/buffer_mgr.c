#include "stdio.h"
#include "stdlib.h"
#include "string.h"

#include "buffer_mgr.h"
#include "storage_mgr.h"

/*Structure for PageFrame*/
typedef struct PageFrame
{
	int pageNum;  	//Page Number of the page in frame
	char *data;			//Data present in frame
	int frameNum;		//Number allocated to frame
	int countSet;		//Checks whether page is being used
	int dirtyFlag;		//Checks if page was modified
	struct pageFrame *next, *prev;	//Used to point to previous and next frame
}PageFrame;

/*Structure for Buffer Pool */
typedef struct BM_BufferPool_Custom
{
	PageFrame *head,*tail,*start;	//Tracks the header, tail and start nodes in a linked list
	PageNumber *fStats;	//used to store page number statistics
	int occupiedCount;		//Stores count of how many frames are occupied
	void *stratData;		//to pass parameters for page replacement strategies
	int numPagesRead;				//Contains total number of pages read from the buffer pool
	int numPagesWritten;				//Contains total number of pages written into the buffer pool
	int *countSet;				//Stores statistics of count sets
	bool *dirtyBit;				//Stores statistics of dirty bits
	
}BM_BufferPool_Custom;


/* Creates a Buffer Pool with specified number of Page Frames*/
void buffPoolCreate(BM_BufferPool_Custom *info)
{
	//Creation of frame and memory allocation
	PageFrame *frame = (PageFrame *) malloc(sizeof(PageFrame));

	//memory allocation for a page in the frame
	frame->data = calloc(PAGE_SIZE,sizeof(char*));


	//Initializing page properties
	frame->dirtyFlag = 0;	
	frame->pageNum = -1;
	frame->frameNum = 0;
	frame->countSet = 0;
	
	//initialise the pointers
	info->head = info->start;

	//if it is the 1st frame make it the HEAD node of Linked List
	if(info->head == NULL)
	{
		info->head = frame;
		info->tail = frame;
		info->start = frame;
	}
	else		//if other than 1st node, appened the nodes to the HEAD node, and make the link between these nodes
	{
		info->tail->next = frame;
		frame->prev = info->tail;
		info->tail = info->tail->next;
	}

	//initialise the other pointers of the linked list
	info->tail->next = info->head;
	info->head->prev = info->tail;
}


/*
creates a new buffer pool with numPages page frames
  */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy,void *stratData)
{
	

	SM_FileHandle fHandle;

	int i;
	//Allocation of memory
	BM_BufferPool_Custom *bpool = (BM_BufferPool_Custom*)malloc(sizeof(BM_BufferPool_Custom));

	//open the page file
	openPageFile((char*) pageFileName,&fHandle);

	//creation of frames
	for(i=0;i<numPages;i++)
	{
		buffPoolCreate(bpool);
	}

	//initialize the values 
	bm->numPages = numPages;
	bm->pageFile = (char*) pageFileName;
	bm->strategy = strategy;
	bm->mgmtData = bpool;




	bpool->tail = bpool->head;
	bpool->stratData = stratData;


	bpool->numPagesRead = 0;
	bpool->numPagesWritten = 0;

	bpool->occupiedCount = 0;

	

	//close the page file
	closePageFile(&fHandle);

	return RC_OK;
}

/*
  This function is used to destroy the buffer pool 
  All the resources that are allocated  are freed 
  DirtyPages are written back, before destroying
 */
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	//load the mgmt data of the buffer pool
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;

	//pointing to head node
	PageFrame *f = bpool->head;

	//writing the dirtyPages back again, before they are destroyed
	forceFlushPool(bm);

	//Freeing the data in the frame
	while(f!=bpool->head)
	{
		free(f->data);
		f= f->next;
	}

	//make the values NULL
	bpool->start = NULL;
	bpool->head = NULL;
	bpool->tail = NULL;

	//free the entire frame
	free(f);

	//free the bufferPool
	free(bpool);

	//setting values to 0/NULL
	bm->numPages = 0;
	bm->mgmtData = NULL;
	bm->pageFile = NULL;

	return RC_OK;
}

/*
 causes all dirty pages (with count set 0) from the buffer pool to be written to
disk.
*/
RC forceFlushPool(BM_BufferPool *const bm)
{

	SM_FileHandle fileHandle;
    BM_BufferPool_Custom *bpool = bm->mgmtData;

    // Point to the head node
    PageFrame *f = bpool->head;

    

    // Open the page file
    RC rc = openPageFile((char *)(bm->pageFile), &fileHandle);
    if (rc != RC_OK)
    {
        return rc;
    }

    // Iterate through all frames 
    while (f != NULL)
    {
        // Check if dirtyFlag is set and countSet is 0
        if (f->dirtyFlag == 1 && f->countSet == 0)
        {
            // Write the page to disk
            rc = writeBlock(f->pageNum, &fileHandle, f->data);
            if (rc != RC_OK)
            {
                closePageFile(&fileHandle);
                return rc;
            }
			// Set the dirtyFlag as 0
            f->dirtyFlag = 0;   
            bpool->numPagesWritten++;
        }
        f = f->next;   // Move to the next frame
        if (f == bpool->head)
        {
            break; //come out of loop once one full loop is completed
        }
    }

    // Close the page file
    rc = closePageFile(&fileHandle);
    if (rc != RC_OK)
    {
        return rc;
    }

    return RC_OK;
}



//Used to mark the page as dirty
 
 
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;
	PageFrame *f = bpool->head;

	do
	{
		//check if the pageNum is same as the page to be marked dirty
		if(page->pageNum == f->pageNum)
		{
			//Set the dirty bit
			f->dirtyFlag = 1;
			return RC_OK;
		}
		f=f->next;
	}while(f!=bpool->head);

	return RC_OK;
}


//Used to unpin the page

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;
	PageFrame *f = bpool->head;

	do
	{
		if(page->pageNum == f->pageNum)
		{
			//decrement countSet
			f->countSet--;
			return RC_OK;
		}
		f = f->next;
	}while(f!= bpool->head);

	return RC_OK;
}

//writes the current content of the page back to the page file on disk
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_BufferPool_Custom *bpool = bm->mgmtData;
    SM_FileHandle fh;

    if (openPageFile((char *)(bm->pageFile), &fh) != RC_OK) {
        return RC_FILE_NOT_FOUND;
    }

    PageFrame *f = bpool->head;
    while (f != bpool->head) {
        if (f->dirtyFlag == 1 && f->pageNum == page->pageNum) {
            if (writeBlock(f->pageNum, &fh, f->data) != RC_OK) {
                closePageFile(&fh);
                return RC_WRITE_FAILED;
            }
            bpool->numPagesWritten++;
            f->dirtyFlag = 0;
            break;
        }
        f = f->next;
        if (f == bpool->head) {
            //If required page is not found, exit loop
            break;
        }
    }

    closePageFile(&fh);
    return RC_OK;
}




//Implementation of First in First Out Page Replacement Stratergy


RC FIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	SM_FileHandle fileHandle;
	BM_BufferPool_Custom *bpool = bm->mgmtData;
	PageFrame *f = bpool->head;

	openPageFile((char*) bm->pageFile,&fileHandle);

	// check if buffer pool has data present 
	do
	{
		//write  the data to the page and increment coutSet
		if(f->pageNum == pageNum)
		{
			page->pageNum = pageNum;
			page->data = f->data;

			f ->pageNum = pageNum;
			f->countSet++;
			return RC_OK;
		}
		f = f->next;
	}while(f!= bpool->head);

	/*if there are frames in the buffer pool that arent occupied
	pin the pages in empty spaces */
	if(bm->numPages>bpool->occupiedCount )
	{
		f = bpool->head;
		f->pageNum = pageNum;

		//move the header to next empty space
		if(bpool->head != f->next)
		{
			bpool->head = f->next;
		}
		f->countSet++;
		bpool->occupiedCount++;	
	}
	else		// FIFO
	{
		//replace pages from frame
		f = bpool->tail;
		do
		{
			//move to page that isnt being used and then replace page
			if(f->countSet != 0)
			{
				f = f->next;
			}
			else
			{
				/* Before page is replaced, check for dirty flag. If true, write to the disk and then perform replacement*/
				if(f->dirtyFlag == 1)
				{
					ensureCapacity(f->pageNum, &fileHandle);
					if(writeBlock(f->pageNum,&fileHandle, f->data)!=RC_OK)
					{
						closePageFile(&fileHandle);
						return RC_WRITE_FAILED;
					}
					bpool->numPagesWritten++;
				}

				//update  attributes
				f->pageNum = pageNum;
				f->countSet++;
				bpool->tail = f->next;
				bpool->head = f;

				break;
			}
		}while(bpool->head!=f );
	}

	//ensure that required number of pages are available, create in case they arent 
	ensureCapacity((pageNum+1),&fileHandle);

	//read the block into pageFrame
	if(readBlock(pageNum, &fileHandle,f->data)!=RC_OK)
	{
		closePageFile(&fileHandle);
		return RC_READ_NON_EXISTING_PAGE;
	}

	bpool->numPagesRead++;

	//update the attributes and put the data onto the page
	page->pageNum = pageNum;
	page->data = f->data;

	//close the pageFile
	closePageFile(&fileHandle);

	return RC_OK;
}







//Implementation of Least Recently Used Page Replacement Stratergy

RC LRU(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{

	SM_FileHandle fileHandle;

	BM_BufferPool_Custom *bpool = bm->mgmtData;
	PageFrame *f = bpool->head;
	
	openPageFile((char*)bm->pageFile,&fileHandle);

	//check if the frame is present in the buffer pool
	do
	{
		if(pageNum == f->pageNum)
		{
			//update  attributes
			page->pageNum = pageNum;
			page->data = f->data;

			f->pageNum = pageNum;
			f->countSet++;

			//point the head and tail for replacement
			bpool->tail = bpool->head->next;
			bpool->head = f;
			return RC_OK;
		}

		f = f->next;

	}while(bpool->head!= f);

	/*if there are frames in the buffer pool that arent occupied
	pin the pages in empty spaces */
	if(bm->numPages > bpool->occupiedCount)
	{

		f = bpool->head;
		f->pageNum = pageNum;

		if(bpool->head  != f->next)
		{
			bpool->head = f->next;
		}
		f->countSet++;		//increment countSet
		bpool->occupiedCount++;	//increment the occupied Count
	}
	else
	{
		//LRU 
		f = bpool->tail;
		do
		{
			//move to page that isnt being used and then replace page
			if(f->countSet != 0)
			{
				f = f->next;
			}
			else
			{
				/* Before page is replaced, check for dirty flag. If true, write to the disk and then perform replacement*/
				if(f->dirtyFlag == 1)
				{
					ensureCapacity(f->pageNum, &fileHandle);
					if(writeBlock(f->pageNum,&fileHandle, f->data)!=RC_OK)
					{
						closePageFile(&fileHandle);
						return RC_WRITE_FAILED;
					}
					bpool->numPagesWritten++;	//increment number of writes 
				}

				//find the least recently used page and replace that page
				if(bpool->tail != bpool->head)
				{
					f->pageNum = pageNum;
					f->countSet++;
					bpool->tail = f;
					bpool->tail = f->next;
					break;
				}
				else
				{
					f = f->next;
					f->pageNum = pageNum;
					f->countSet++;
					bpool->tail = f;
					bpool->head = f;
					bpool->tail = f->prev;
					break;
				}
			}
		}while(bpool->tail!=f);
	}

	ensureCapacity((pageNum+1),&fileHandle);
	if(readBlock(pageNum, &fileHandle,f->data)!=RC_OK)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	bpool->numPagesRead++;

	//update the page frame and its data
	page->pageNum = pageNum;
	page->data = f->data;

	//close the pagefile
	closePageFile(&fileHandle);

	return RC_OK;
}


/*
pins the page with page number pageNum
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	// Chosing the  Page Replacement Strategy 
	
	if(bm->strategy==RS_FIFO)
	{
	
		FIFO(bm, page, pageNum);
	}

	else
	{
		LRU(bm,page,pageNum);

	}
	return RC_OK;
}




// Statistics 


/*
 The getFrameContents function returns an array of PageNumbers (of size numPages)
  where the ith element is the number of the page stored in the ith page frame.
  An empty page frame is represented using the constant NO_PAGE.
 */

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	BM_BufferPool_Custom *bpool;
	int i;
	int page_count = bm->numPages;

	bpool = bm->mgmtData;
	bpool->fStats = (PageNumber*)malloc(sizeof(PageNumber)*bm->numPages);
	
	PageFrame *f = bpool->start;
	PageNumber* frameData = bpool->fStats;

	i=0;

	if(frameData != NULL)
	{
		for(i=0;i< page_count;i++)
		{
			frameData[i] = f->pageNum;
			f = f->next;
		}
	}

	return frameData;
}

/*
 The getDirtyFlags function returns an array of bools (of size numPages)
  where the ith element is TRUE if the page stored in the ith page frame is dirty.
 Empty page frames are considered as clean.
 */
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;
	bpool->dirtyBit = (bool*)malloc(sizeof(bool)*bm->numPages);

	PageFrame *f = bpool->start;
	bool* dirtyBit = bpool->dirtyBit;

	int i;
	int pageCount;
	pageCount = bm->numPages;

	if(dirtyBit != NULL)
	{
		for(i=0;i< pageCount;i++)
		{
			dirtyBit[i] = f->dirtyFlag;
			f = f->next;
		}
	}
	//free(bpool->dirtyBit);

	return dirtyBit;
}

/*
  The getFixCounts function returns an array of ints (of size numPages)
 where the ith element is the fix count of the page stored in the ith page frame.
 Return 0 for empty page frames.
 */
int *getFixCounts (BM_BufferPool *const bm)
{
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;
	bpool->countSet = (int*)malloc(sizeof(int)*bm->numPages);

	PageFrame *f = bpool->start;
	int* countSet = bpool->countSet;

	int i;
	int pageCount;
	pageCount = bm->numPages;

	if(countSet != NULL)
	{
		for(i=0;i< pageCount;i++)
		{
			countSet[i] = f->countSet;
			f = f->next;
		}
	}

	return  countSet;
}

/* returns the number of pages that have been read from disk since a
buffer pool has been initialized*/

int getNumReadIO (BM_BufferPool *const bm)
{
	return ((BM_BufferPool_Custom*)bm->mgmtData)->numPagesRead;
}

/* Returns the number of pages written to the page file since the buffer pool has been
initialized */
int getNumWriteIO (BM_BufferPool *const bm)
{
	return ((BM_BufferPool_Custom*)bm->mgmtData)->numPagesWritten;
}
