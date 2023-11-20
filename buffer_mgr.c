#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RC_QUEUE_IS_EMPTY 5;
#define RC_NO_FREE_BUFFER_ERROR 6;

typedef struct pHandle {
	char *bufDat;
	int isDirty;
	int fixCount;
	int pageNum;
	int frameNum;
	struct pHandle *next;
	struct pHandle *prev;
} pHandle;

typedef struct Queue {
	pHandle *head;
	pHandle *tail;
	int occupiedFrame;
	int totFrame;
} Queue;

pHandle* createFrame(pHandle *);
pHandle* createBufferOfSize(int ,pHandle *);

SM_FileHandle *fHandle;
Queue *q;
int r,w;

RC LRU(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum);
RC FIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum);




void cQueue(BM_BufferPool *const bm){
	pHandle *p[bm->numPages];
	int lastPage = (bm->numPages) - 1;
	int k;
	for (k = 0; k <= lastPage; k++) {
		p[k] = (pHandle*) malloc(sizeof(pHandle));
	}
	for (k = 0; k <= lastPage; k++) {
		p[k]->frameNum = k;
		p[k]->isDirty = 0;
		p[k]->fixCount = 0;
		p[k]->pageNum = -1;
		p[k]->bufDat = (char*) calloc(PAGE_SIZE, sizeof(char));
	}
	int i;
	for (i = 0; i <= lastPage; i++) {
		int k = i;
		if (k == 0)
		{
			p[k]->prev = NULL;
			p[k]->next = p[k + 1];
		}

		else if (k == lastPage) {
			p[k]->next = NULL;
			p[k]->prev = p[k - 1];
		}
		else {

			p[k]->next = p[k + 1];
			p[k]->prev = p[k - 1];
		}
	}
	q->head = p[0];
	q->tail = p[lastPage];
	q->occupiedFrame = 0;
	q->totFrame = bm->numPages;
}

RC clearQueue() {
	return (q->occupiedFrame == 0);
}

pHandle* ListCreate(const PageNumber pageNum) {
	pHandle* nInf = (pHandle*) malloc(sizeof(pHandle));
	char *c = (char*) calloc(PAGE_SIZE, sizeof(char));

	nInf->pageNum = pageNum;
	nInf->isDirty = 0;
	nInf->frameNum = 0;
	nInf->fixCount = 1;
	nInf->bufDat = c;
	nInf->prev=NULL;
	nInf->next = NULL;
	
	return nInf;
}



RC remQueue() {
	if (clearQueue()) {
		return RC_QUEUE_IS_EMPTY;
	}

	pHandle *p = q->head;
	int i;
	for (i = 0; i < q->occupiedFrame; i++) {
		if (i == (q->occupiedFrame-1)) {
			q->tail = p;
		} else
			p = p->next;
	}
	

	int tnum; 
	int deletedPage=0;
	pHandle *pageInf = q->tail;
	for ( i = 0; i < q->totFrame; i++) {

		if ((pageInf->fixCount) == 0) {

			if (pageInf->pageNum == q->tail->pageNum) {
				deletedPage=pageInf->pageNum;
				q->tail = (q->tail->prev);
				q->tail->next = NULL;
 
			} else {
				deletedPage=pageInf->pageNum;
				pageInf->prev->next = pageInf->next;
				pageInf->next->prev = pageInf->prev;
			}

		} else {
			tnum=pageInf->pageNum;
			pageInf = pageInf->prev;

		}
	}

	if (tnum == q->tail->pageNum){
		return 0;		

	}

	if (pageInf->isDirty == 1) {
		writeBlock(pageInf->pageNum, fHandle, pageInf->bufDat);
		w++;
	}

	q->occupiedFrame--;

	return deletedPage;
}

RC addQueue(BM_PageHandle * const page, const PageNumber pageNum,BM_BufferPool * const bm) {

	int deletedPage=-1;
	if (q->occupiedFrame == q->totFrame ) { 
		deletedPage=remQueue();
	}

	pHandle* pageInf = ListCreate(pageNum);

	if (clearQueue()) {

		readBlock(pageInf->pageNum,fHandle,pageInf->bufDat);
		page->data = pageInf->bufDat;
		r++;

		pageInf->frameNum = q->head->frameNum;
		pageInf->next = q->head;
		q->head->prev = pageInf;
		pageInf->pageNum = pageNum;
		page->pageNum= pageNum;
		q->head = pageInf;


	} else {  
		readBlock(pageNum, fHandle, pageInf->bufDat);
		if(deletedPage==-1)
			pageInf->frameNum = q->head->frameNum+1;
		else
			pageInf->frameNum=deletedPage;
		page->data = pageInf->bufDat;
		r++;
		pageInf->next = q->head;
		q->head->prev = pageInf;
		q->head = pageInf;
		page->pageNum= pageNum;
		

	}
	q->occupiedFrame++;

	return RC_OK; 
}

	RC LRU(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {

	int pFound = 0;
	pHandle *pageInf = q->head;
	int i;
	for ( i= 0; i < bm->numPages; i++) {
		if (pFound == 0) {
			if (pageInf->pageNum == pageNum) {
				pFound = 1;
				break;
			}
			else
			pageInf = pageInf->next;
		}
	}

	if (pFound == 0)
		addQueue(page,pageNum,bm);

	if (pFound == 1) {
		pageInf->fixCount++;
		page->data = pageInf->bufDat;
		page->pageNum=pageNum;
		if (pageInf == q->head) {
			pageInf->next = q->head;
			q->head->prev = pageInf;
			q->head = pageInf;
		}

		if (pageInf != q->head) {
			pageInf->prev->next = pageInf->next;
			if (pageInf->next) {
				pageInf->next->prev = pageInf->prev;

				if (pageInf == q->tail) {
					q->tail = pageInf->prev;
					q->tail->next = NULL;
				}
				pageInf->next = q->head;
				pageInf->prev = NULL;
				pageInf->next->prev = pageInf;
				q->head = pageInf;
			}
		}
	}

	return RC_OK;
}


RC FIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
		int pFound = 0;
		int numPages;
		numPages = bm->numPages;
			pHandle *l=NULL,*t=NULL,*temp1=NULL;
			l = q->head;
			int i;
			for (i = 0; i < numPages; i++) {
				if (pFound == 0) {
					if (l->pageNum == pageNum) {
						pFound = 1;
						break;
					}
					else
					l = l->next;
				}
			}

			if (pFound == 1 )
			{
				l->fixCount++;
				page->data = l->bufDat;
				page->pageNum = pageNum;

				return RC_OK;
			}
				t = q->head;
				int rcode = -1;
				
				
									

			while (q->occupiedFrame < q->totFrame)
			{
				if(t->pageNum == -1)
				{
			t->fixCount = 1;
			t->isDirty = 0;
			t->pageNum = pageNum;
			page->pageNum= pageNum;
			q->occupiedFrame = q->occupiedFrame + 1 ;
			
			readBlock(t->pageNum,fHandle,t->bufDat);
			page->data = t->bufDat;
			r++;
			rcode = 0;
			break;
 				}
			else
				t = t->next;
			}
			
			if(rcode == 0)
			return RC_OK;
		
		
			pHandle *additionalNode = (pHandle *) malloc (sizeof(pHandle));
			additionalNode->fixCount = 1;
			additionalNode->isDirty = 0;
			additionalNode->pageNum = pageNum;
			additionalNode->bufDat = NULL;
			additionalNode->next = NULL;
			page->pageNum= pageNum;
			additionalNode->prev = q->tail;
			t = q->head;
			for(i=0; i<numPages ;i++)
			{
				if((t->fixCount)== 0)
					break;
	        else
				t = t->next;
			}

			if(i==numPages)
			{
				return RC_NO_FREE_BUFFER_ERROR;
			}

			temp1=t;
				if(t == q->head)
				{

					q->head = q->head->next;
					q->head->prev = NULL;

				}
				else if(t == q->tail)
				{
					q->tail = t->prev;
					additionalNode->prev=q->tail;
				}
				else{
					t->prev->next = t->next;
					t->next->prev=t->prev;
					}

				if(temp1->isDirty == 1)
				{
				 writeBlock(temp1->pageNum,fHandle,temp1->bufDat);
				 w++;
				}
			additionalNode->bufDat = temp1->bufDat;
			additionalNode->frameNum = temp1->frameNum;
			
			readBlock(pageNum,fHandle,additionalNode->bufDat);
			page->data = additionalNode->bufDat;
			r++;
			
			q->tail->next = additionalNode;
			q->tail=additionalNode;
           return RC_OK;

}

RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum){
	int res;
	if(bm->strategy==RS_FIFO)
		res=FIFO(bm,page,pageNum);
	else
		res=LRU(bm,page,pageNum);
	return res;
}


void updateBuff(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy){

	 char* bs = (char *)calloc(numPages,sizeof(char)*PAGE_SIZE);

	   bm->pageFile = (char *)pageFileName;
	   bm->numPages = numPages;
	   bm->strategy = strategy;
	   bm->mgmtData = bs;
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
       	    r = 0;
	    w = 0;
	   fHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	   q = (Queue *)malloc(sizeof(Queue));

	  updateBuff(bm,pageFileName,numPages,strategy);

	  openPageFile(bm->pageFile,fHandle);

	  cQueue(bm);

	   return RC_OK;

}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	int i;
	int rcode = -1;
	pHandle *pageInf=NULL,*t=NULL;

	pageInf=q->head;
	for(i=0; i< q->occupiedFrame ; i++)
	{
		if(pageInf->fixCount==0 && pageInf->isDirty == 1)
		{
			writeBlock(pageInf->pageNum,fHandle,pageInf->bufDat);
			w++;
			pageInf->isDirty=0;
			}
			pageInf=pageInf->next;		
	}


	closePageFile(fHandle);
	
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
	int i;
	pHandle *temp1;
	temp1 = q->head;
	for(i=0; i< q->totFrame; i++)
	{
		if((temp1->isDirty==1) && (temp1->fixCount==0))
		{
			writeBlock(temp1->pageNum,fHandle,temp1->bufDat);
			w++;
			temp1->isDirty=0;

		}

		temp1=temp1->next;
	}
	return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	pHandle *t;
	int i;
	t = q->head;
	
	for(i=0; i < bm->numPages; i++){
		if(t->pageNum==page->pageNum)
			break;
		t=t->next;
	}
	
	if(i == bm->numPages)
		return RC_READ_NON_EXISTING_PAGE;		
	else
		t->fixCount--;
	return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	pHandle *t;
	int i;
	t = q->head;
	
	for(i=0; i < bm->numPages; i++){
		if(t->pageNum==page->pageNum)
			break;
		t=t->next;
	}
	
	int flag;

	if(i == bm->numPages)
		return 1;          
	if((flag=writeBlock(t->pageNum,fHandle,t->bufDat))==0)
		w++;
	else
		return RC_WRITE_FAILED;

	return RC_OK;
}






RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	pHandle *t;
	int i;
	t = q->head;
	
	for(i=0; i < bm->numPages; i++){
		if(t->pageNum==page->pageNum)
			break;
		if(t->next!=NULL)
		t=t->next;
	}
	
	if(i == bm->numPages)
	return RC_READ_NON_EXISTING_PAGE;
	t->isDirty=1;
	return RC_OK;
}


PageNumber *getFrameContents (BM_BufferPool *const bm){
	PageNumber (*p)[bm->numPages];
	int i;
	p=calloc(bm->numPages,sizeof(PageNumber));
	pHandle *t;
	for(i=0; i< bm->numPages;i++){	
       for(t=q->head ; t!=NULL; t=t->next){
	           if(t->frameNum ==i){
		       (*p)[i] = t->pageNum;
			   break;
				}
			}
		}
	return *p;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
	bool (*b)[bm->numPages];
	int i;
	b=calloc(bm->numPages,sizeof(PageNumber));
	pHandle *t;
	
	for(i=0; i< bm->numPages ;i++){
		for(t=q->head ; t!=NULL; t=t->next){
           if(t->frameNum ==i){
		if(t->isDirty==1)
			(*b)[i]=TRUE;
		else
			(*b)[i]=FALSE;
		break;
	}
		}
	}
	return *b;

}

int *getFixCounts (BM_BufferPool *const bm){
	int (*fc)[bm->numPages];
	int i;
	fc=calloc(bm->numPages,sizeof(PageNumber));
	pHandle *t;

	for(i=0; i< bm->numPages;i++){	
       for(t=q->head ; t!=NULL; t=t->next){
	           if(t->frameNum ==i){
		       (*fc)[i] = t->fixCount;
			   break;
				}
			}
		}
	return *fc;
}

int getNumReadIO (BM_BufferPool *const bm){
	return r;
}

int getNumWriteIO (BM_BufferPool *const bm){
	return w;
}

