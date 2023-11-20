#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

void sRead(RM_TableData *, BM_PageHandle *);
char * rName(char *);
char * rMetaData(char *);
int rKeys(char *);
char * rKeyData(char *);
char * singleAttribute(char *, int );
char ** getName(char *, int );
char * fetchName(char *);
int fetchType(char *);
int * getType(char *, int );
int fetchLength(char *data);
int * getLen(char *sDat, int numAtr);
int * fetchKeyData(char *data,int keyNum);
int * fetchPSlot(char *);
char * readPSlot(char *);
int getOfst(Schema *, int );

int tombStone[10000];


typedef struct tInf{
    int rSize;
    int numRec;
    int bfac;
    RID fLoc;
    RM_TableData *rtData;
    BM_PageHandle pHandle;
    BM_BufferPool bp;
}tInf;

typedef struct RM_SCAN_MGMT{
    RID recordID;
    Expr *c;
    int count;  
    RM_TableData *rtData;
    BM_PageHandle rHandle;
    BM_BufferPool bPool;
}RM_SCAN_MGMT;


tInf tInfo;
RM_SCAN_MGMT rScan;
SM_FileHandle fHandle;
SM_PageHandle pHandle;



RC initRecordManager (void *mgmtData){
	
    initStorageManager();
    int x;
	for(x=0;x<10000;x++)
		tombStone[x]= -99;
	return RC_OK;
}

RC shutdownRecordManager (){
    if(pHandle != ((char *)0)){
        free(pHandle);
    }
    return RC_OK;
}


RC createTable (char *name, Schema *schema){

    if(createPageFile(name) != RC_OK){
        return 1;
    }
    pHandle = (SM_PageHandle) malloc(PAGE_SIZE);

    char td[PAGE_SIZE];
    memset(td,'\0',PAGE_SIZE);

    sprintf(td,"%s|",name);

    int rs = getRecordSize(schema);
    sprintf(td+ strlen(td),"%d[",schema->numAttr);
int i;
    for(i=0; i<schema->numAttr; i++){
        sprintf(td+ strlen(td),"(%s:%d~%d)",schema->attrNames[i],schema->dataTypes[i],schema->typeLength[i]);
    }
    sprintf(td+ strlen(td),"]%d{",schema->keySize);

    for(i=0; i<schema->keySize; i++){
        sprintf(td+ strlen(td),"%d",schema->keyAttrs[i]);
        if(i<(schema->keySize-1))
            strcat(td,":");
    }
    strcat(td,"}");

    tInfo.fLoc.page =1;
    tInfo.fLoc.slot =0;
    tInfo.numRec =0;

    sprintf(td+ strlen(td),"$%d:%d$",tInfo.fLoc.page,tInfo.fLoc.slot);

    sprintf(td+ strlen(td),"?%d?",tInfo.numRec);

    memmove(pHandle,td,PAGE_SIZE);

    if (openPageFile(name, &fHandle) != RC_OK) {
        return 1;
    }

    if (writeBlock(0, &fHandle, pHandle)!= RC_OK) {
        return 1;
    }


    free(pHandle);
    return RC_OK;
}

RC openTable (RM_TableData *rel, char *name){
    pHandle = (SM_PageHandle) malloc(PAGE_SIZE);

    BM_PageHandle *h = &tInfo.pHandle;

    BM_BufferPool *bman = &tInfo.bp;

    initBufferPool(bman, name, 3, RS_FIFO, NULL);

    if(pinPage(bman, h, 0) != RC_OK){
        RC_message = "Pin page failed ";
        return RC_PIN_PAGE_FAILED;
    }


    sRead(rel,h);

    if(unpinPage(bman,h) != RC_OK){
        RC_message = "Unpin page failed ";
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}


RC closeTable (RM_TableData *rel){
    char td[PAGE_SIZE];
    BM_PageHandle *page = &tInfo.pHandle;
    BM_BufferPool *bman = &tInfo.bp;
    char *pDat;  
    memset(td,'\0',PAGE_SIZE);

    sprintf(td,"%s|",rel->name);

    int rs = tInfo.rSize;
    sprintf(td+ strlen(td),"%d[",rel->schema->numAttr);
int i;
    for(i=0; i<rel->schema->numAttr; i++){
        sprintf(td+ strlen(td),"(%s:%d~%d)",rel->schema->attrNames[i],rel->schema->dataTypes[i],rel->schema->typeLength[i]);
    }
    sprintf(td+ strlen(td),"]%d{",rel->schema->keySize);

    for(i=0; i<rel->schema->keySize; i++){
        sprintf(td+ strlen(td),"%d",rel->schema->keyAttrs[i]);
        if(i<(rel->schema->keySize-1))
            strcat(td,":");
    }
    strcat(td,"}");

    sprintf(td+ strlen(td),"$%d:%d$",tInfo.fLoc.page,tInfo.fLoc.slot);

    sprintf(td+ strlen(td),"?%d?",tInfo.numRec);



    if(pinPage(bman,page,0) != RC_OK){
        return RC_PIN_PAGE_FAILED;
    }

    memmove(page->data,td,PAGE_SIZE);

    if( markDirty(bman,page)!=RC_OK){
        return RC_MARK_DIRTY_FAILED;
    }

    if(  unpinPage(bman,page)!=RC_OK){
        return RC_UNPIN_PAGE_FAILED;
    }


    if(shutdownBufferPool(bman) != RC_OK){
        return RC_BUFFER_SHUTDOWN_FAILED;
    }

    return RC_OK;
}



RC deleteTable (char *name){
    BM_BufferPool *bman = &tInfo.bp;
    if(name == ((char *)0)){
        return RC_NULL_IP_PARAM;
    }

    if(destroyPageFile(name) != RC_OK){
        return RC_FILE_DESTROY_FAILED;
    }

    return RC_OK;
}



int getNumTuples (RM_TableData *rel){

    return tInfo.numRec;
}





RC insertRecord (RM_TableData *rel, Record *record){

    char *pDat;   
    int rs = tInfo.rSize;
    int fpNum = tInfo.fLoc.page;  
    int fsNum = tInfo.fLoc.slot;  
    int bfac = tInfo.bfac;
    BM_PageHandle *page = &tInfo.pHandle;
    BM_BufferPool *bman = &tInfo.bp;

    if(fpNum < 1 || fsNum < 0){
        return RC_IVALID_PAGE_SLOT_NUM;
    }

    if(pinPage(bman,page,fpNum) != RC_OK){
        return RC_PIN_PAGE_FAILED;
    }


    pDat = page->data;  

    int ofst =  fsNum * rs; 

    record->data[rs-1]='$';
    memcpy(pDat+ofst, record->data, rs);

    if( markDirty(bman,page)!=RC_OK){
        return RC_MARK_DIRTY_FAILED;
    }

    if(  unpinPage(bman,page)!=RC_OK){
        return RC_UNPIN_PAGE_FAILED;
    }


    record->id.page = fpNum;  
    record->id.slot = fsNum;  


    tInfo.numRec = tInfo.numRec +1;

    if(fsNum ==(bfac-1)){
        tInfo.fLoc.page=fpNum+1;
        tInfo.fLoc.slot =0;
    }else{
        tInfo.fLoc.slot = fsNum +1;
    }
    return RC_OK;
}



RC deleteRecord (RM_TableData *rel, RID id){
    int rs = tInfo.rSize;
    int rpNum = id.page;  
    int rsNum = id.slot;  
    int bfac = tInfo.bfac;
    BM_PageHandle *page = &tInfo.pHandle;
    BM_BufferPool *bman = &tInfo.bp;


    if(pinPage(bman,page,rpNum) != RC_OK){
        return RC_PIN_PAGE_FAILED;
    }

    int rOffset = rsNum * rs;

    memset(page->data+rOffset, '\0', rs);  

    tInfo.numRec = tInfo.numRec -1;  

    if(markDirty(bman,page)!=RC_OK){
        return RC_MARK_DIRTY_FAILED;
    }
    if(unpinPage(bman,page)!=RC_OK){
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}


RC updateRecord (RM_TableData *rel, Record *record){
    int rs = tInfo.rSize;
    int rpNum = record->id.page;  
    int rsNum = record->id.slot;  
    int bfac = tInfo.bfac;
    BM_PageHandle *page = &tInfo.pHandle;
    BM_BufferPool *bman = &tInfo.bp;

    if(pinPage(bman,page,rpNum) != RC_OK){
        return RC_PIN_PAGE_FAILED;
    }
    int rOffset = rsNum * rs;

    memcpy(page->data+rOffset, record->data, rs-1); 
    if( markDirty(bman,page)!=RC_OK){
        return RC_MARK_DIRTY_FAILED;
    }

    if(  unpinPage(bman,page)!=RC_OK){
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}



RC getRecord (RM_TableData *rel, RID id, Record *record){


    int rs = tInfo.rSize;
    int rpNum = id.page;  
    int rsNum = id.slot;  
    int bfac = tInfo.bfac;
    BM_PageHandle *page = &tInfo.pHandle;
    BM_BufferPool *bman = &tInfo.bp;

    if(pinPage(bman,page,rpNum) != RC_OK){
        return RC_PIN_PAGE_FAILED;
    }

    int rOffset = rsNum * rs;  
    memcpy(record->data, page->data+rOffset, rs); 
    record->data[rs-1]='\0';

    record->id.page = rpNum;
    record->id.slot = rsNum;


    if(  unpinPage(bman,page)!=RC_OK){
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}


RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *c){
    BM_BufferPool *bman = &tInfo.bp;

    scan->rel = rel;
    rScan.c=c;
    rScan.recordID.page=1;
    rScan.recordID.slot=0; 
    rScan.count = 0;

    scan->mgmtData = &rScan;

    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record){
    if(tInfo.numRec < 1 || rScan.count==tInfo.numRec)
        return  RC_RM_NO_MORE_TUPLES;

    int bfac = tInfo.bfac;
    int totTup = tInfo.numRec;
    BM_PageHandle *page = &tInfo.pHandle;
    BM_BufferPool *bman = &tInfo.bp;
    int crScan = rScan.count; 
    int cpScan = rScan.recordID.page;  
    int csScan = rScan.recordID.slot;  
    Value *qRes = (Value *) malloc(sizeof(Value));
    rScan.count = rScan.count +1 ;

    while(crScan<totTup){

        rScan.recordID.page= cpScan;
        rScan.recordID.slot= csScan;

        if(getRecord(scan->rel,rScan.recordID,record) != RC_OK){
        }
        crScan = crScan+1;   

        if (rScan.c != NULL){
            evalExpr(record, (scan->rel)->schema, rScan.c, &qRes);
            if(qRes->v.boolV ==1){
                record->id.page=cpScan;
                record->id.slot=csScan;
                if(csScan ==(bfac-1)){
                    cpScan =cpScan +1  ;
                    csScan = 0;
                }else{
                    csScan = csScan +1;
                }
                rScan.recordID.page= cpScan;
                rScan.recordID.slot= csScan;
                return RC_OK;
            }
        }else{
            qRes->v.boolV = TRUE; 
        }
        if(csScan ==(bfac-1)){
            cpScan =cpScan +1  ;
            csScan = 0;
        }else{
            csScan = csScan +1;
        }
    }
    qRes->v.boolV = TRUE;
    rScan.recordID.page=1; 
    rScan.recordID.slot=0;
    rScan.count = 0;
    return  RC_RM_NO_MORE_TUPLES;

}

RC closeScan (RM_ScanHandle *scan){
    rScan.recordID.page=1; 
    rScan.recordID.slot=0; 
    rScan.count = 0;
    return RC_OK;
}



int getRecordSize (Schema *schema){
    if(schema ==((Schema *)0)){
        return RC_SCHEMA_NOT_INIT;
    }
    int rs = 0;
int i;
    for(i=0; i<schema->numAttr; i++){
        switch(schema->dataTypes[i]){
            case DT_INT:
                rs = rs + sizeof(int);
                break;
            case DT_STRING:
                rs = rs + (sizeof(char) * schema->typeLength[i]);
                break;
            case DT_FLOAT:
                rs = rs + sizeof(float);
                break;
            case DT_BOOL:
                rs = rs + sizeof(bool);
                break;
        }
    }
    return rs;
}


Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

    Schema *schema = (Schema * ) malloc(sizeof(Schema));

    

    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    int rs = getRecordSize(schema);
    tInfo.rSize = rs;
    tInfo.numRec = 0;

    return schema;
}


RC freeSchema (Schema *schema){
    free(schema);
    return RC_OK;
}



RC createRecord (Record **record, Schema *schema){

    Record *nr = (Record *) malloc (sizeof(Record));
    if(nr == ((Record *)0)){
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }

    nr->data = (char *)malloc(sizeof(char) * tInfo.rSize);
    memset(nr->data,'\0',sizeof(char) * tInfo.rSize);

    nr->id.page =-1;           
    nr->id.page =-1;           

    *record = nr;

    return RC_OK;
}


RC freeRecord (Record *record){
    if(record == ((Record *)0)){
        return RC_NULL_IP_PARAM;
    }
    if(record->data != ((char *)0))
        free(record->data);
    free(record);
    return RC_OK;
}


RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    int ofst = getOfst(schema,attrNum);

    Value *lr;
    int ia;
    float fa;
    int sz =0;
    char *s ;


    switch (schema->dataTypes[attrNum]) {
        case DT_INT :
            sz = sizeof(int);
            s= malloc(sz+1);    
            memcpy(s, record->data+ofst, sz);
            s[sz]='\0';         
            ia =  atoi(s);
            MAKE_VALUE(*value, DT_INT, ia);
            free(s);
            break;
        case DT_STRING :
            sz =sizeof(char)*schema->typeLength[attrNum];
            s= malloc(sz+1);    
            memcpy(s, record->data+ofst, sz);
            s[sz]='\0';      

            MAKE_STRING_VALUE(*value, s);
            free(s);
            break;
        case DT_FLOAT :
            sz = sizeof(float);
            s= malloc(sz+1);    
            memcpy(s, record->data+ofst, sz);
            s[sz]='\0';         
            fa =  atof(s);

            MAKE_VALUE(*value, DT_FLOAT, fa);
            free(s);
            break;
        case DT_BOOL :
            sz = sizeof(bool);
            s= malloc(sz+1);    
            memcpy(s, record->data+ofst, sz);
            s[sz]='\0';         
            ia =  atoi(s);

            MAKE_VALUE(*value, DT_BOOL, ia);
            free(s);
            break;
    }

    return RC_OK;

}

void repString(int x,int val,  char *intString){
    int r=0;
    int p = val;
    int l = x;
    while (p > 0 && x >= 0) {
        r = p % 10;
        p = p / 10;
        intString[x] = intString[x] + r;

        x--;
    }
    intString[l+1] = '\0';

}




int readAtt(char *sDat){
    char *sAttribute = (char *) malloc(sizeof(int)*2);
    memset(sAttribute,'\0',sizeof(int)*2);
    int i=0;
    int x=0;
    for(i=0; sDat[i] != '|'; i++){
    }
    i++;
    while(sDat[i] != '['){
        sAttribute[x++]=sDat[i++];
    }
    sAttribute[x]='\0';
    return atoi(sAttribute);
}




int fetchRecTot(char *sDat){
    char *dat = (char *) malloc(sizeof(char)*10);
    memset(dat,'\0',sizeof(char)*10);
    int i=0;
    while(sDat[i] != '?'){
        i++;
    }
    i++;
    int x=0;
    for(x=0;sDat[i] != '?'; x++){
        dat[x] = sDat[i++];
    }
    dat[x]='\0';

    return atoi(dat);
}



void sRead(RM_TableData *rel, BM_PageHandle *h){
    char metadata[PAGE_SIZE];
    strcpy(metadata,h->data);

    char *sname=rName(metadata);


    int totAttr = readAtt(metadata);


    char *attMData =rMetaData(metadata);


    int totkeyAttribute = rKeys(metadata);


    char *atrKeydt = rKeyData(metadata);


    char *fSlot = readPSlot(metadata);


    char **names=getName(attMData,totAttr);
    DataType *dt =   getType(attMData,totAttr);
    int *sizes = getLen(attMData,totAttr);
    int *keys =   fetchKeyData(atrKeydt,totkeyAttribute);
    int *pSlot = fetchPSlot(fSlot);
    int totaltuples = fetchRecTot(metadata);


    int i;
    char **cpNames = (char **) malloc(sizeof(char*) * totAttr);
    DataType *cpDt = (DataType *) malloc(sizeof(DataType) * totAttr);
    int *cpSizes = (int *) malloc(sizeof(int) * totAttr);
    int *cpKeys = (int *) malloc(sizeof(int)*totkeyAttribute);
    char *cpSName = (char *) malloc(sizeof(char)*20);

    memset(cpSName,'\0',sizeof(char)*20);
    for(i = 0; i < totAttr; i++)
    {
        cpNames[i] = (char *) malloc(sizeof(char) * 10);
        strcpy(cpNames[i], names[i]);
    }

    memcpy(cpDt, dt, sizeof(DataType) * totAttr);
    memcpy(cpSizes, sizes, sizeof(int) * totAttr);
    memcpy(cpKeys, keys, sizeof(int) * totkeyAttribute);
    memcpy(cpSName,sname,strlen(sname));

    free(names);
    free(dt);
    free(sizes);
    free(keys);
    free(sname);

    Schema *schema = createSchema(totAttr, cpNames, cpDt, cpSizes, totkeyAttribute, cpKeys);
    rel->schema=schema;
    rel->name =cpSName;

    tInfo.rtData = rel;
    tInfo.rSize =  getRecordSize(rel->schema) + 1;   //
    tInfo.bfac = (PAGE_SIZE / tInfo.rSize);
    tInfo.fLoc.page =pSlot[0];
    tInfo.fLoc.slot =pSlot[1];
    tInfo.numRec = totaltuples;

}


char * rName(char *sDat){
    char *tName = (char *) malloc(sizeof(char)*20);
    memset(tName,'\0',sizeof(char)*20);
    int i=0;
    for(i=0; sDat[i] != '|'; i++){
        tName[i]=sDat[i];
    }
    tName[i]='\0';
    return tName;
}




int rKeys(char *sDat){
    char *sAttribute = (char *) malloc(sizeof(int)*2);
    memset(sAttribute,'\0',sizeof(int)*2);
    int i=0;
    int x=0;
    for(i=0; sDat[i] != ']'; i++){
    }
    i++;
    while(sDat[i] != '{'){
        sAttribute[x++]=sDat[i++];
    }
    sAttribute[x]='\0';
    return atoi(sAttribute);
}

char * rMetaData(char *sDat){
    char *dat = (char *) malloc(sizeof(char)*100);
    memset(dat,'\0',sizeof(char)*100);
    int i=0;
    while(sDat[i] != '['){
        i++;
    }
    i++;
    int x=0;
    for(x=0;sDat[i] != ']'; x++){
        dat[x] = sDat[i++];
    }
    dat[x]='\0';

    return dat;

}


char * rKeyData(char *sDat){
    char *dat = (char *) malloc(sizeof(char)*50);
    memset(dat,'\0',sizeof(char)*50);
    int i=0;
    while(sDat[i] != '{'){
        i++;
    }
    i++;
    int x=0;
    for(x=0;sDat[i] != '}'; x++){
        dat[x] = sDat[i++];
    }
    dat[x]='\0';

    return dat;
}

char * readPSlot(char *sDat){
    char *dat = (char *) malloc(sizeof(char)*50);
    memset(dat,'\0',sizeof(char)*50);
    int i=0;
    while(sDat[i] != '$'){
        i++;
    }
    i++;
    int x=0;
    for(x=0;sDat[i] != '$'; x++){
        dat[x] = sDat[i++];
    }
    dat[x]='\0';

    return dat;
}


char ** getName(char *sDat, int numAtr){

    char ** attrName = (char **) malloc(sizeof(char)*numAtr);
    int i;
	for(i=0; i<numAtr; i++){
        char *atrDt =singleAttribute(sDat,i);
        char *name = fetchName(atrDt);
        attrName[i] = malloc(sizeof(char) * strlen(name));
        strcpy(attrName[i],name);
        free(name);
        free(atrDt);
    }
    return attrName;
}

int * getType(char *sDat, int numAtr){
    int *tpDt=(int *) malloc(sizeof(int) *numAtr);
int i;
    for(i=0; i<numAtr; i++){
        char *atrDt =singleAttribute(sDat,i);
        tpDt[i]  = fetchType(atrDt);

        free(atrDt);
    }
    return tpDt;
}

int * getLen(char *sDat, int numAtr){
    int *szDt= (int *) malloc(sizeof(int) *numAtr);
int i;
    for(i=0; i<numAtr; i++){
        char *atrDt =singleAttribute(sDat,i);
        szDt[i]  = fetchLength(atrDt);
     
        free(atrDt);
    }

    return szDt;
}

char * singleAttribute(char *sDat, int atrNum){
    char *dat = (char *) malloc(sizeof(char)*30);
    int count=0;
    int i=0;
    while(count<=atrNum){
        if(sDat[i++] == '(')
            count++;
    }
    int x=0;
    for(x=0;sDat[i] != ')'; x++){
        dat[x] = sDat[i++];
    }
    dat[x]='\0';
    return dat;
}


char * fetchName(char *data){
    char *name = (char *) malloc(sizeof(char)*10);
    memset(name,'\0',sizeof(char)*10);
    int i;
    for(i=0; data[i]!=':'; i++){
        name[i] = data[i];
    }
    name[i]='\0';
    return  name;
}

int fetchType(char *data){
    char *dtTp = (char *) malloc(sizeof(int)*2);
    memset(dtTp,'\0',sizeof(char)*10);
    int i;
    int x;
    for(i =0 ; data[i]!=':'; i++){
    }
    i++;
    for(x=0; data[i]!='~'; x++){
        dtTp[x]=data[i++];
    }
    dtTp[x]='\0';
    int dt =atoi(dtTp);
    free(dtTp);
    return  dt;
}

int fetchLength(char *data){
    char *dtLen = (char *) malloc(sizeof(int)*2);
    memset(dtLen,'\0',sizeof(char)*10);
    int i, x;
    for(i =0 ; data[i]!='~'; i++){
    }
    i++;
    for(x=0; data[i]!='\0'; x++){
        dtLen[x]=data[i++];
    }
    dtLen[x]='\0';
    int dt =atoi(dtLen);
    free(dtLen);
    return  dt;
}

int * fetchKeyData(char *data,int keyNum){
    char *val = (char *) malloc(sizeof(int)*2);
    int * val2=(int *) malloc(sizeof(int) *keyNum);
    memset(val,'\0',sizeof(int)*2);
    int i=0;
    int x=0;
	int k;
    for(k=0; data[k]!='\0'; k++){
        if(data[k]==':' ){
            val2[x]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
            i=0;
            x++;

        }else{
            val[i++] = data[k];
        }

    }
    val2[keyNum-1] =atoi(val);

    return  val2;
}


int * fetchPSlot(char *data){
    char *val = (char *) malloc(sizeof(int)*2);
    int * val2=(int *) malloc(sizeof(int) *2);
    memset(val,'\0',sizeof(int)*2);
    int i=0;
    int x=0;
	int k;
    for(k=0; data[k]!='\0'; k++){
        if(data[k]==':' ){
            val2[x]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
            i=0;
            x++;

        }else{
            val[i++] = data[k];
        }

    }
    val2[1] =atoi(val);
    printf("\n Slot %d",val2[1]);
    return  val2;
}



int getOfst(Schema *schema, int atrnum){
    int ofst=0;
int p;
    for(p=0; p<atrnum; p++){
        switch(schema->dataTypes[p]){
            case DT_INT:
                ofst = ofst + sizeof(int);
                break;
            case DT_STRING:
                ofst = ofst + (sizeof(char) *  schema->typeLength[p]);
                break;
            case DT_FLOAT:
                ofst = ofst + sizeof(float);
                break;
            case DT_BOOL:
                ofst = ofst  + sizeof(bool);
                break;
        }
    }

    return ofst;
}

