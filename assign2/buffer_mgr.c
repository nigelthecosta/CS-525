/*
 * buffer_mgr.c
 */

#include "stdio.h"
#include "stdlib.h"
#include "string.h"

#include "buffer_mgr.h"
#include "storage_mgr.h"

/*Structure for PageFrame inside BufferPool*/
typedef struct PageFrame
{
	int pageNum;  	//Page Number of the Page present in the Page frame
	char *data;			//Actual data present in the page
	int frameNum;		//number associated with each Page frame
	int countSet;		//Fix count to mark whether the page is in use by other users	
	int dirtyFlag;		//Dirty flag to determine whether page was modified/write
	struct pageFrame *next, *prev;	//Nodes of the Doubly linked List where each node is a frame, pointing to other frames
}PageFrame;

/*Structure for Buffer Pool to store Management Information*/
typedef struct BM_BufferPool_Custom
{
	PageFrame *head,*tail,*start;	//keep track of nodes in linked list
	PageNumber *frameContent;	//an array of page numbers to store the statistics of number of pages stored in the page frame
	int occupiedCount;		//to keep count of number of frames occupied inside the pool
	void *stratData;		//to pass parameters for page replacement strategies
	int numPagesRead;				//to give total number of pages read from the buffer pool
	int numPagesWritten;				//to give total number of pages wrote into the buffer pool
	int *countSet;				//an array of integers to store the statistics of fix counts for a page
	bool *dirtyBit;				//an array of bool's to store the statistics of dirty bits for modified page
	
}BM_BufferPool_Custom;


/*This function is used to create a Buffer Pool with specified number of Page Frames
 i.e linked list of frames with some default values, with the first frame acting as the head node
 while the last acting as the tail node.
 This function is called by the initBufferPool() function passing mgmt info as the parameter*/
void createPageFrame(BM_BufferPool_Custom *info)
{
	//Create a frame and assign a memory to it
	PageFrame *frame = (PageFrame *) malloc(sizeof(PageFrame));

	//allocate memory for page to stored into the pageFrame
	frame->data = calloc(PAGE_SIZE,sizeof(char*));


	//intialise the page properties of the frames i.e each frame has a page within,
	//so properties (default page values) are applied to the frames itself
	frame->dirtyFlag = 0;	//FALSE
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

// Buffer Manager Interface Pool Handling

/*
 * This function is used to create a Buffer Pool for an existing page file
 * bm -> used to store the mgmtData
 * pageFileName -> name of the page file, whose pages are to be cached
 * numPages -> number of frames in the buffer Pool
 * strategy -> Page Replacement Strategy to be used
 * stratData -> parameters if required for any page replacement strategy
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy,void *stratData)
{
	

	//Storage manager file handle
	SM_FileHandle fHandle;

	int i;
	//Memory allocation to store the Buffer Pool Management Data
	BM_BufferPool_Custom *bpool = (BM_BufferPool_Custom*)malloc(sizeof(BM_BufferPool_Custom));

	//open the page file, whose pages are to be cached
	openPageFile((char*) pageFileName,&fHandle);

	//create the frames for buffer pool
	for(i=0;i<numPages;i++)
	{
		createPageFrame(bpool);
	}

	//initialize the values and store it in management data
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
 * This function is used to destroy the buffer pool (bm)
 * All the resources that are allocated i.e all memory allocation are free'd to avoid memory leaks
 * All the dirtyPages are written back, before destroying
 */
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	//load the mgmt data of the buffer pool
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;

	//point to the head node
	PageFrame *f = bpool->head;

	//calling forceFlush which writes all the dirtyPages back again, before destroying
	forceFlushPool(bm);

	//free all the page data in the frame
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

	//set all the values to 0 or NULL
	bm->numPages = 0;
	bm->mgmtData = NULL;
	bm->pageFile = NULL;

	return RC_OK;
}

/*
 * This function is used to write all the pages to the disk whose dirtyBit is set
 */


RC forceFlushPool(BM_BufferPool *const bm)
{

	SM_FileHandle fileHandle;
    // Load the management data
    BM_BufferPool_Custom *bpool = bm->mgmtData;

    // Point to the head node
    PageFrame *f = bpool->head;

    

    // Open the page file
    RC rc = openPageFile((char *)(bm->pageFile), &fileHandle);
    if (rc != RC_OK)
    {
        return rc;
    }

    // Iterate through all frames in the buffer pool
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
            f->dirtyFlag = 0;   // Mark the dirtyFlag as 0
            bpool->numPagesWritten++;
        }
        f = f->next;   // Move to the next frame
        if (f == bpool->head)
        {
            break; // We have completed a full loop through all frames
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

// Buffer Manager Interface Access Pages

/*
 * This function is used to mark the page as dirty
 * The page of BM_PageHandle's is marked with dirty bit set to 1
 */
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
			//mark it's dirty bit
			f->dirtyFlag = 1;
			return RC_OK;
		}
		f=f->next;
	}while(f!=bpool->head);

	return RC_OK;
}

/*
 * This function is used to unpinpage
 * After the user/client has done with reading of page it is set free, i.e its countSet is decremented
 * using unpinpage called as "UNPINNING"
 */
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;
	PageFrame *f = bpool->head;

	do
	{
		if(page->pageNum == f->pageNum)
		{
			//decrement fix count
			f->countSet--;
			return RC_OK;
		}
		f = f->next;
	}while(f!= bpool->head);

	return RC_OK;
}

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
            // We have completed one full loop without finding the desired page,
            // so we break to avoid an infinite loop.
            break;
        }
    }

    closePageFile(&fh);
    return RC_OK;
}

/*
 * This function is used to put a page onto the bufferPool
 * Each page is put in a pageFrame, which is put onto the bufferPool
 * This method is called as "PINNING" a page.
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	// Choose the appropriate Page Replacement Strategy
	// We have implemented FIFO, LRU and CLOCK strategy

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

// FIFO Page Replacement Strategy Implementation
/*
 * This function pinPageFIFO is implementation of First In First Out (FIFO)
 * Here, we have implemented Queue implementation for FIFO
 */





RC FIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	SM_FileHandle fileHandle;
	BM_BufferPool_Custom *bpool = bm->mgmtData;
	PageFrame *f = bpool->head;

	openPageFile((char*) bm->pageFile,&fileHandle);

	// if page is already present in the buffer pool
	do
	{
		//put the data onto the page and increment the fix count
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

	//if there are remaining frames in the buffer pool, i.e. bufferpool is not fully occupied
	//pin the pages in the empty spaces
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
		bpool->occupiedCount++;	//increment the occupied count
	}
	else		//use page replacement strategy FIFO
	{
		//replace pages from frame
		f = bpool->tail;
		do
		{
			//check if the page is in use, i.e. countSet > 0
			//goto next frame whose fix count = 0, and replace the page
			if(f->countSet != 0)
			{
				f = f->next;
			}
			else
			{
				//before replacing check for dirtyflag if dirty write back to disk and then replace
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

				//update the frame and bufferPool attributes
				f->pageNum = pageNum;
				f->countSet++;
				bpool->tail = f->next;
				bpool->head = f;

				break;
			}
		}while(bpool->head!=f );
	}

	//ensure if the pageFile has the required number of pages, if not create those
	ensureCapacity((pageNum+1),&fileHandle);

	//read the block into pageFrame
	if(readBlock(pageNum, &fileHandle,f->data)!=RC_OK)
	{
		closePageFile(&fileHandle);
		return RC_READ_NON_EXISTING_PAGE;
	}

	//increment the num of read operations
	bpool->numPagesRead++;

	//update the attributes and put the data onto the page
	page->pageNum = pageNum;
	page->data = f->data;

	//close the pageFile
	closePageFile(&fileHandle);

	return RC_OK;
}







// LRU Page Replacement Strategy Implementations
/*
 * This method implements LRU page replacement strategy
 * i.e. the page which is Least Recently Used will be replaced, with the new page
 */


RC LRU(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{

	SM_FileHandle fileHandle;

	BM_BufferPool_Custom *bpool = bm->mgmtData;
	PageFrame *f = bpool->head;
	
	openPageFile((char*)bm->pageFile,&fileHandle);

	//check if frame already in buffer pool
	do
	{
		if(pageNum == f->pageNum)
		{
			//update the page and frame attributes
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

	//if there are empty spaces in the bufferPool , then fill in those frames first
	if(bm->numPages > bpool->occupiedCount)
	{

		f = bpool->head;
		f->pageNum = pageNum;

		if(bpool->head  != f->next)
		{
			bpool->head = f->next;
		}
		f->countSet++;		//increment the fix count
		bpool->occupiedCount++;	//increment the occupied Count
	}
	else
	{
		//replace pages from frame using LRU
		f = bpool->tail;
		do
		{
			//check if page in use, move onto next page to be replaced
			if(f->countSet != 0)
			{
				f = f->next;
			}
			else
			{
				//before replacing check if dirty flag set, write back content onto the disk
				if(f->dirtyFlag == 1)
				{
					ensureCapacity(f->pageNum, &fileHandle);
					if(writeBlock(f->pageNum,&fileHandle, f->data)!=RC_OK)
					{
						closePageFile(&fileHandle);
						return RC_WRITE_FAILED;
					}
					bpool->numPagesWritten++;	//increment number of writes performed
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
		}while(f!= bpool->tail);
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





// Statistics Interface
/*
 * The getFrameContents function returns an array of PageNumbers (of size numPages)
 * where the ith element is the number of the page stored in the ith page frame.
 * An empty page frame is represented using the constant NO_PAGE.
 */
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;
	bpool->frameContent = (PageNumber*)malloc(sizeof(PageNumber)*bm->numPages);

	PageFrame *frame = bpool->start;
	PageNumber* frameContents = bpool->frameContent;

	int i;
	int page_count = bm->numPages;

	if(frameContents != NULL)
	{
		for(i=0;i< page_count;i++)
		{
			frameContents[i] = frame->pageNum;
			frame = frame->next;
		}
	}
	//free(bpool->frameContent);

	return frameContents;
}

/*
 * The getDirtyFlags function returns an array of bools (of size numPages)
 * where the ith element is TRUE if the page stored in the ith page frame is dirty.
 * Empty page frames are considered as clean.
 */
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;
	bpool->dirtyBit = (bool*)malloc(sizeof(bool)*bm->numPages);

	PageFrame *frame = bpool->start;
	bool* dirtyBit = bpool->dirtyBit;

	int i,page_count = bm->numPages;

	if(dirtyBit != NULL)
	{
		for(i=0;i< page_count;i++)
		{
			dirtyBit[i] = frame->dirtyFlag;
			frame = frame->next;
		}
	}
	//free(bpool->dirtyBit);

	return dirtyBit;
}

/*
 * The getFixCounts function returns an array of ints (of size numPages)
 * where the ith element is the fix count of the page stored in the ith page frame.
 * Return 0 for empty page frames.
 */
int *getFixCounts (BM_BufferPool *const bm)
{
	BM_BufferPool_Custom *bpool;
	bpool = bm->mgmtData;
	bpool->countSet = (int*)malloc(sizeof(int)*bm->numPages);

	PageFrame *frame = bpool->start;
	int* countSet = bpool->countSet;

	int i,page_count = bm->numPages;

	if(countSet != NULL)
	{
		for(i=0;i< page_count;i++)
		{
			countSet[i] = frame->countSet;
			frame = frame->next;
		}
	}
	//free(bpool->countSet);

	return  countSet;
}

/*
 * This function gets the total number of ReadBlock operations performed
 */
int getNumReadIO (BM_BufferPool *const bm)
{
	return ((BM_BufferPool_Custom*)bm->mgmtData)->numPagesRead;
}

/*
 * This function gets the total number of writeBlock operations performed
 */
int getNumWriteIO (BM_BufferPool *const bm)
{
	return ((BM_BufferPool_Custom*)bm->mgmtData)->numPagesWritten;
}
