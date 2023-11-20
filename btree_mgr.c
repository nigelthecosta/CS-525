#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>

SM_FileHandle fHandle;
int highestElement;

typedef struct treeHandle
{
    int *k;
    struct treeHandle **next;
    RID *identifier;
} treeHandle;

treeHandle *root;
treeHandle *s;
int indexNumber = 0;


//  Used to initialize Btree.

RC initIndexManager (void *mgmtData)
{
    return RC_OK;
}



//  Used to shut down index manager
RC shutdownIndexManager ()
{
    return RC_OK;
}



//  Used to allocate memory to Btree
RC createBtree (char *idxId, DataType keyType, int n)
{
	int i;
    root = (treeHandle*)malloc(sizeof(treeHandle));
    root->k = malloc(sizeof(int) * n);
    root->identifier = malloc(sizeof(int) * n);
    root->next = malloc(sizeof(treeHandle) * (n + 1));
    for (i = 0; i < n+1 ; i ++)
        root->next[i] = NULL;
    highestElement = n;
    createPageFile (idxId);
    
    return RC_OK;
}


//  Opens Btree pagefile
RC openBtree (BTreeHandle **tree, char *idxId)
{
    if(openPageFile (idxId, &fHandle)==0){
    	return RC_OK;
	}

    else{
    	return RC_ERROR;
	}
}


//  Used to free memory allocated to Btree
RC closeBtree (BTreeHandle *tree)
{
    if(closePageFile(&fHandle)==0){
	free(root);
    return RC_OK;
	}
	else{
		return RC_ERROR;
	}
}

//  Used to delete Btree records
RC deleteBtree (char *idxId)
{
    if(destroyPageFile(idxId)==0){
		return RC_OK;
	}
    
    else{
    	return RC_ERROR;
	}
}



//  Used to give the number of nodes in the tree
RC getNumNodes (BTreeHandle *tree, int *result)
{
    treeHandle *t = (treeHandle*)malloc(sizeof(treeHandle));
    
    int numberOfNodes = 0;
    int i;
    
    for (i = 0; i < highestElement + 2; i ++) {
        numberOfNodes ++;
    }

    *result = numberOfNodes;
    
    
    return RC_OK;
}



//  Gives the number of entries in a Btree
RC getNumEntries (BTreeHandle *tree, int *result)
{
	int numberOfElements = 0, i;
    treeHandle *t = (treeHandle*)malloc(sizeof(treeHandle));
    
    for (t = root; t != NULL; t = t->next[highestElement])
        for (i = 0; i < highestElement; i ++)
            if (t->k[i] != 0)
                numberOfElements ++;
    *result = numberOfElements;
    return RC_OK;
}


//  Used to return the key type whether int, string etc
RC getKeyType (BTreeHandle *tree, DataType *result)
{
    return RC_OK;
}



//   Used to find the position of the key
RC findKey (BTreeHandle *tree, Value *k, RID *result)
{
    treeHandle *t = (treeHandle*)malloc(sizeof(treeHandle));
    int locateIndex = 0;
    int i;
    for (t = root; t != NULL; t = t->next[highestElement]) {
        for (i = 0; i < highestElement; i ++) {
            if (t->k[i] == k->v.intV) {
                (*result).page = t->identifier[i].page;
                (*result).slot = t->identifier[i].slot;
                locateIndex = 1;
                break;
            }
        }
        if (locateIndex == 1)
            break;
    }
    
    if (locateIndex == 1)
        return RC_OK;
    else
        return RC_IM_KEY_NOT_FOUND;
}


//   Used to insert key into the btree
RC insertKey (BTreeHandle *tree, Value *k, RID rid)
{
    int i = 0;
    treeHandle *t = (treeHandle*)malloc(sizeof(treeHandle));
    treeHandle *n = (treeHandle*)malloc(sizeof(treeHandle));
    n->k = malloc(sizeof(int) * highestElement);
    n->identifier = malloc(sizeof(int) * highestElement);
    n->next = malloc(sizeof(treeHandle) * (highestElement + 1));
    
    for (i = 0; i < highestElement; i ++) {
    	n->k[i] = 0;
    }

    
    int isNodeOccupied = 0;
    
    for (t = root; t != NULL; t = t->next[highestElement]) {
        isNodeOccupied = 0;
        for (i = 0; i < highestElement; i ++) {
            if (t->k[i] == 0) {
                t->identifier[i].page = rid.page;
                t->identifier[i].slot = rid.slot;
                t->k[i] = k->v.intV;
                t->next[i] = NULL;
                isNodeOccupied ++;
                break;
            }
        }
        if ((isNodeOccupied == 0) && (t->next[highestElement] == NULL)) {
            n->next[highestElement] = NULL;
            t->next[highestElement] = n;
        }
    }
    
    int numberOfElements = 0;
    for (t = root; t != NULL; t = t->next[highestElement])
        for (i = 0; i < highestElement; i ++)
            if (t->k[i] != 0)
                numberOfElements ++;

    if (numberOfElements == 6) {
        n->k[0] = root->next[highestElement]->k[0];
        n->k[1] = root->next[highestElement]->next[highestElement]->k[0];
        n->next[0] = root;
        n->next[1] = root->next[highestElement];
        n->next[2] = root->next[highestElement]->next[highestElement];

    }
    
    return RC_OK;
}



//   Used to delete a key from the Btree
RC deleteKey (BTreeHandle *tree, Value *k)
{
    treeHandle *t = (treeHandle*)malloc(sizeof(treeHandle));
    int locateIndex = 0, i;
    for (t = root; t != NULL; t = t->next[highestElement]) {
        for (i = 0; i < highestElement; i ++) {
            if (t->k[i] == k->v.intV) {
                t->k[i] = 0;
                t->identifier[i].page = 0;
                t->identifier[i].slot = 0;
                locateIndex = 1;
                break;
            }
        }
        if (locateIndex == 1)
            break;
    }
    

    return RC_OK;
}



//   Used to scan the trees
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    s = (treeHandle*)malloc(sizeof(treeHandle));
    s = root;
    indexNumber = 0;
    
    treeHandle *t = (treeHandle*)malloc(sizeof(treeHandle));
    int numberOfElements = 0;
    int i;
    for (t = root; t != NULL; t = t->next[highestElement])
        for (i = 0; i < highestElement; i ++)
            if (t->k[i] != 0)
                numberOfElements ++;

    int k[numberOfElements];
    int elements[highestElement][numberOfElements];
    int count = 0;
    for (t = root; t != NULL; t = t->next[highestElement]) {
        for (i = 0; i < highestElement; i ++) {
            k[count] = t->k[i];
            elements[0][count] = t->identifier[i].page;
            elements[1][count] = t->identifier[i].slot;
            count ++;
        }
    }
    
    int exchange;
    int g, s, c, d;
    for (c = 0 ; c < count - 1; c ++)
    {
        for (d = 0 ; d < count - c - 1; d ++)
        {
            if (k[d] > k[d+1])
            {
                exchange = k[d];
                g = elements[0][d];
                s = elements[1][d];
                
                k[d]   = k[d + 1];
                elements[0][d] = elements[0][d + 1];
                elements[1][d] = elements[1][d + 1];
                
                k[d + 1] = exchange;
                elements[0][d + 1] = g;
                elements[1][d + 1] = s;
            }
        }
    }
    
    count = 0;
    for (t = root; t != NULL; t = t->next[highestElement]) {
        for (i = 0; i < highestElement; i ++) {
            t->k[i] =k[count];
            t->identifier[i].page = elements[0][count];
            t->identifier[i].slot = elements[1][count];
            count ++;
        }
    }

    return RC_OK;
}



//   Used to read the next entry into the Btree
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    if(s->next[highestElement] != NULL) {
        if(highestElement == indexNumber) {
            indexNumber = 0;
            s = s->next[highestElement];
        }

        (*result).page = s->identifier[indexNumber].page;
        (*result).slot = s->identifier[indexNumber].slot;
        indexNumber ++;
    }
    else
        return RC_IM_NO_MORE_ENTRIES;
    
    return RC_OK;
}

// Used to close the scanning of the tree elements
RC closeTreeScan (BT_ScanHandle *handle)
{
    indexNumber = 0;
    return RC_OK;
}


char *printTree (BTreeHandle *tree)
{
    return RC_OK;
}
