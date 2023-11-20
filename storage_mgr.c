#include "storage_mgr.h"
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include "test_helper.h"

typedef struct mdList 
{
        char k[50];
        char v[50];
        struct mdList *next;
} mdList;

mdList *first = NULL;
mdList *curr = NULL;
mdList *prev = NULL;

int wc = 1; 

void initStorageManager()
{
    
}


void getMd(int n, char * string,char *kvp)
{
    char ns[PAGE_SIZE];
    memset(ns, '\0', PAGE_SIZE);
    ns[0] = ';';
    strcat(ns, string);

    char pos[1000];
    int i;
    int del = 0;

    for (i = 0; i < strlen(ns); i++)
    {
        if (ns[i] == ';')
        {
            pos[del] = i;
            del++;
        }
    }

    int cpos = 0;
    for (i = pos[n - 1] + 1;
            i <= pos[n] - 1; i++)
    {
        kvp[cpos] = ns[i];
        cpos++;
    }
    kvp[cpos] = '\0';
}


mdList * conList(char *mInfo,
        int conNodes)
{
    int iL;
    char cmkv[100];

    char ck[50];
    memset(ck,'\0',50);

    char cv[50];
    memset(cv,'\0',50);

    for (iL = 1; iL <= conNodes; iL++)
    {
        memset(cmkv,'\0',100);
        getMd(iL, mInfo,cmkv);

        char colonFound = 'N';
        int kCnt = 0;
        int vCnt = 0;
        int i;
        for (i = 0; i < strlen(cmkv); i++)
        {
            if (cmkv[i] == ':')
                colonFound = 'Y';

            if (colonFound == 'N')
                ck[kCnt++] = cmkv[i];
            else if (cmkv[i] != ':')
                cv[vCnt++] = cmkv[i];
        }
        ck[kCnt] = '\0';
        cv[vCnt] = '\0';


        curr = (mdList *) malloc(sizeof(mdList));

        strcpy(curr->v,cv);
        strcpy(curr->k,ck);
        curr->next = NULL;

        if (iL == 1)
        {
            first= curr;
            prev = NULL;
        }
        else
        {
            prev->next = curr;
        }
        prev = curr;
    }
    return first;
}





RC createPageFile(char *filename)
{
    FILE *fp;
    fp = fopen(filename, "a+b"); 

    if (wc == 1) 
    {
        if (fp != NULL)
        {
            char ns2[PAGE_SIZE]; 
            char ns3[PAGE_SIZE]; 

            char stringPageSize[5];
            sprintf(stringPageSize, "%d", PAGE_SIZE);

            char stringM[PAGE_SIZE * 2];
            strcpy(stringM, "PS:"); 
            strcat(stringM, stringPageSize);
            strcat(stringM, ";");
            strcat(stringM, "NP:0;"); 

            int i;
            for (i = strlen(stringM); i < (PAGE_SIZE * 2); i++)
                stringM[i] = '\0';
            memset(ns2, '\0', PAGE_SIZE);
            memset(ns3, '\0', PAGE_SIZE);

            fwrite(stringM, PAGE_SIZE, 1, fp);
            fwrite(ns2, PAGE_SIZE, 1, fp);
            fwrite(ns3, PAGE_SIZE, 1, fp);

            fclose(fp);
            return RC_OK;
        } else
        {
            return RC_FILE_NOT_FOUND;
        }
    }
    else
    {
        if (fp != NULL)
        {
            char ns[PAGE_SIZE];

            memset(ns, '\0', PAGE_SIZE);
            fwrite(ns, PAGE_SIZE, 1, fp);

            fclose(fp);
            return RC_OK;
        } else
        {
            return RC_FILE_NOT_FOUND;
        }
    }

}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    struct stat s;
    FILE *fp;

    fp = fopen(fileName, "r");
    if (fp != NULL)
    {
        fHandle->fileName = fileName;
        fHandle->curPagePos = 0;
        stat(fileName, &s);
        fHandle->totalNumPages = (int) s.st_size / PAGE_SIZE;
        fHandle->totalNumPages -= 2; 

        char mString[PAGE_SIZE * 2];
        fgets(mString, (PAGE_SIZE * 2), fp);

        int i;
        int n = 0;
        for (i = 0; i < strlen(mString); i++)
            if (mString[i] == ';')
                n++;

        fHandle->mgmtInfo = conList(
                mString, n);

        fclose(fp);

        return RC_OK;
    }
    else
    {
        return RC_FILE_NOT_FOUND;
    }
}

void cString(int n,char * ra)
{
    char a[4];
    memset(a, '\0', 4);
    int i = 0;
    while (n != 0)
    {
        a[i++] = (n % 10) + '0';
        n /= 10;
    }
    a[i] = '\0';

    int j=0;
    int x;
    for(x = strlen(a)-1;x>=0;x--)
    {
        ra[j++] = a[x];
    }
    ra[j]='\0';
}

RC wMeta(SM_FileHandle *fHandle,char *d)
{
    FILE *fp = fopen(fHandle->fileName,"r+b");

    if(fp != NULL)
    {
        fwrite(d,1,PAGE_SIZE,fp);
        fclose(fp);
        return RC_OK;
    }
    else
    {
        return RC_WRITE_FAILED;
    }
}


void memFree()
{
    mdList *prev;
    mdList *current  = first;
    prev = first;
    while(current != NULL)
    {
        current = current->next;
        if(prev!=NULL)
            free(prev);
        prev = current;
    }
    prev = NULL;
    first = NULL;
}

RC closePageFile(SM_FileHandle *fHandle)
{
    if (fHandle != NULL)
    {
        mdList *temp = first;
        char string[4];
        memset(string,'\0',4);
        while (1 == 1)
        {
            if(temp != NULL)
            {
                
                    if (strcmp(temp->k, "NP") == 0)
                    {
                        cString(fHandle->totalNumPages,string);
                        strcpy(temp->v,string);
                        break;
                    }
                
                temp = temp->next;
            }
            else
                break;
        }
        temp = first;

        char metaData[2 * PAGE_SIZE];
        memset(metaData, '\0', 2 * PAGE_SIZE);
        int i = 0;
        while (temp != NULL)
        {
            int kc = 0;
            int vc = 0;
            while (temp->k[kc] != '\0')
                metaData[i++] = temp->k[kc++];
            metaData[i++] = ':';
            while (temp->v[vc] != '\0')
                metaData[i++] = temp->v[vc++];
            metaData[i++] = ';';
            temp = temp->next;
        }
        wMeta(fHandle,metaData);
        fHandle->curPagePos = 0;
        fHandle->fileName = NULL;
        fHandle->mgmtInfo = NULL;
        fHandle->totalNumPages = 0;
        fHandle = NULL;
        memFree();
        return RC_OK;
    }
    else
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

RC destroyPageFile(char *fileName)
{
    if (remove(fileName) == 0)
        return RC_OK;
    else
        return RC_FILE_NOT_FOUND;
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{

    if (pageNum < 0 || pageNum > fHandle->totalNumPages)//
        return RC_WRITE_FAILED;
    else
    {
        int startPosition = (pageNum * PAGE_SIZE) + (2 * PAGE_SIZE);

        FILE *fp = fopen(fHandle->fileName, "r+b");
        if (fp != NULL)
        {
            if (fseek(fp, startPosition, SEEK_SET) == 0)
            {
                fwrite(memPage, 1, PAGE_SIZE, fp);
                if (pageNum > fHandle->curPagePos)
                    fHandle->totalNumPages++;
                fHandle->curPagePos = pageNum;
                fclose(fp);
                return RC_OK;
            }
            else
            {
                return RC_WRITE_FAILED;
            }
        } else
        {
            return RC_FILE_HANDLE_NOT_INIT;
        }
    }
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (writeBlock(fHandle->curPagePos, fHandle, memPage) == RC_OK)
        return RC_OK;
    else
        return RC_WRITE_FAILED;
}

RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    wc = 2;
    if (createPageFile(fHandle->fileName) == RC_OK)
    {
        fHandle->totalNumPages++;
        fHandle->curPagePos = fHandle->totalNumPages - 1;
        wc = 1;
        return RC_OK;
    }
    else
    {
        wc = 1;
        return RC_WRITE_FAILED;
    }
}


RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    int pAdd = numberOfPages - (fHandle->totalNumPages);
    int i;
    if (pAdd > 0)
    {
        for (i = 0; i < pAdd; i++)
        {
            wc = 3;
            createPageFile(fHandle->fileName);
            wc = 1;
            fHandle->totalNumPages++;
            fHandle->curPagePos = fHandle->totalNumPages - 1;
        }
        return RC_OK;
    } else
        return RC_READ_NON_EXISTING_PAGE; 
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle->totalNumPages < pageNum)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }
    else
    {
        FILE *fp;
        fp = fopen(fHandle->fileName, "r");
        
        if (fp != NULL)
        {
            if (fseek(fp, ((pageNum * PAGE_SIZE) + 2 * PAGE_SIZE), SEEK_SET)== 0)
            {
                fread(memPage, PAGE_SIZE, 1, fp);
                fHandle->curPagePos = pageNum;
                fclose(fp);
                return RC_OK;
            }
            else
            {
                return RC_READ_NON_EXISTING_PAGE;
            }
        } else
        {
            return RC_FILE_NOT_FOUND;
        }
    }
}

int getBlockPos(SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(0, fHandle, memPage) == RC_OK)
        return RC_OK;
    else
        return RC_READ_NON_EXISTING_PAGE;
}


RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(getBlockPos(fHandle) - 1, fHandle, memPage) == RC_OK)
        return RC_OK;
    else
        return RC_READ_NON_EXISTING_PAGE;
}


RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(getBlockPos(fHandle), fHandle, memPage) == RC_OK)
        return RC_OK;
    else
        return RC_READ_NON_EXISTING_PAGE;
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(getBlockPos(fHandle) + 1, fHandle, memPage) == RC_OK)
        return RC_OK;
    else
        return RC_READ_NON_EXISTING_PAGE;
}


RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(fHandle->totalNumPages - 1, fHandle, memPage) == RC_OK)
        return RC_OK;
    else
        return RC_READ_NON_EXISTING_PAGE;
}

