/*
 * hippo_utils.c
 * Author: Jia Yu jiayu2@asu.edu
 */

#include "postgres.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "catalog/pg_statistic.h"
#include "fmgr.h"
#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "access/hippo.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/elog.h"
#include "catalog/pg_attribute.h"
#include "access/xlogreader.h"
#include "catalog/pg_index.h"
#include "access/htup_details.h"
//#include "hippo_utils.h"
#include "ewok.h"


/*
 * Form a real size HippoTupleLong (long version). This one is current in memory and not serialized but will be serialized to disk right away.
 */
 HippoTupleLong* build_real_hippo_tuplelong(HippoBuildState* buildstate)
{
	HippoTupleLong *hippoTupleLong=palloc(sizeof(HippoTupleLong));
	hippoTupleLong->length=buildstate->hp_length;
	hippoTupleLong->hp_PageStart=buildstate->hp_PageStart;
	hippoTupleLong->hp_PageNum=buildstate->hp_PageNum;
	buildstate->lengthcounter+=hippoTupleLong->length;
	hippoTupleLong->originalBitset=buildstate->originalBitset;
	hippoTupleLong->deleteFlag=buildstate->deleteFlag;
	return hippoTupleLong;
}


/*
 * Copy a HippoTupleLong from one memory space to another memory space
 */
 void copy_hippo_mem_tuple(HippoTupleLong *newTuple,HippoTupleLong* oldTuple)
{
	newTuple->hp_PageStart=oldTuple->hp_PageStart;
	newTuple->hp_PageNum=oldTuple->hp_PageNum;
	newTuple->deleteFlag=oldTuple->deleteFlag;
	newTuple->originalBitset=bitmap_copy(oldTuple->originalBitset);
}


/*
 * Get new buffer if the old buffer is not large enough. This buffer is expected to be used right away because the pin is still on it.
 */
 Buffer
hippo_getinsertbuffer(Relation irel)
{
	Buffer buffer;
	Page page;
	Size pageSize;
	buffer = ReadBuffer(irel, P_NEW);
	if(buffer==NULL)
	{
		elog(ERROR, "[hippo_getinsertbuffer] Initialized buffer NULL");
	}
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	pageSize = BufferGetPageSize(buffer);
	page = BufferGetPage(buffer);
	if(page==NULL)
	{
		elog(ERROR, "[hippo_getinsertbuffer] Initialized page NULL at subfunc");
	}
	/*
	 *Do some page initialization.
	 */
	PageInit(page, pageSize, sizeof(BlockNumber)*2+sizeof(GridList));
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	/*
	 *Note: the pin on this buffer is not removed.
	 */
	LockBuffer(buffer,BUFFER_LOCK_UNLOCK);
	return buffer;

}

/*
 * Form a serialized index tuple. This index tuple will be put on disk right away.
 */
IndexTupleData * hippo_form_indextuple(HippoTupleLong *memTuple, Size *memlen, int histogramBoundsNum)
{
	char	   *ptr,
			   *ret,
			   *diskGridSet,
			   *diskGridSetAccumulator;
	struct ewah_bitmap *compressedBitset;
	Size gridMemLength,bitmapLength;
	/*
	 * The deleteFlag is a flag for distinguishing whether this GSIN tuple is "dirty" or not. And also it is also used to mark whether this is the last index tuple. It is used in bulkdelete.
	 */
	int16 deleteFlag=0;
	*memlen = 0;
	deleteFlag=memTuple->deleteFlag;
	compressedBitset=bitmap_compress(memTuple->originalBitset);
	bitmapLength=estimate_ewah_size(compressedBitset);
	*memlen=sizeof((memTuple->hp_PageStart)&INDEX_SIZE_MASK)+sizeof((memTuple->hp_PageNum)&INDEX_SIZE_MASK)+sizeof(deleteFlag)+bitmapLength;
	ptr = ret = palloc(*memlen);
	memcpy(ptr,&(memTuple->hp_PageStart),sizeof((memTuple->hp_PageStart)&INDEX_SIZE_MASK));
	ptr+=sizeof((memTuple->hp_PageStart)&INDEX_SIZE_MASK);
	memcpy(ptr,&(memTuple->hp_PageNum),sizeof((memTuple->hp_PageNum)&INDEX_SIZE_MASK));
	ptr+=sizeof((memTuple->hp_PageNum)&INDEX_SIZE_MASK);
	memcpy(ptr,&(deleteFlag),sizeof((deleteFlag)));
	ptr+=sizeof((deleteFlag));
	ewah_serialize(compressedBitset,ptr);
	ewah_free(compressedBitset);
	return (IndexTupleData *) ret;
}

/*
 * Create sorted list and put it on disk
 */
void SortedListInialize(Relation index,HippoBuildState *hippoBuildState,BlockNumber startBlock)
{
	Size itemSize;
	BlockNumber maxBlock,currentBlock;
	Offset off;
	int totalIndexTuples,remainder;
	maxBlock=hippoBuildState->hp_indexnumtuples/SORTED_LIST_TUPLES_PER_PAGE;
	remainder=hippoBuildState->hp_indexnumtuples%SORTED_LIST_TUPLES_PER_PAGE;
	totalIndexTuples=hippoBuildState->hp_indexnumtuples;
	/*
	 * Start to initialize the list and put them on disk
	 */
	for(currentBlock=startBlock;currentBlock<=maxBlock+startBlock;currentBlock++){
		SortedListPerIndexPage(hippoBuildState,currentBlock,maxBlock,remainder,startBlock);
	}
}
/*
 * This function puts the sorted list on one disk page.
 */
void SortedListPerIndexPage(HippoBuildState *hippoBuildState,BlockNumber currentBlock,BlockNumber maxBlock,int remainder,BlockNumber startBlock)
{
	char *diskTuple;
	Size len=0;
	int i;
	Buffer currentBuffer;
	Page page;
	int iterationTimes;
	currentBuffer=ReadBuffer(hippoBuildState->hp_irel,currentBlock);
	page=BufferGetPage(currentBuffer);
	if(page==NULL)
	{
		elog(ERROR,"[SortedListPerIndexPage] Got NULL page for sorted list");
	}
	if(currentBlock==startBlock)
	{
		/*
		 * This is the first tuple, put the total number on the first page.
		 */
		OffsetNumber tempOff;
		diskTuple=palloc(sizeof(int));
		memcpy(diskTuple,&(hippoBuildState->hp_indexnumtuples),sizeof(int));
		LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
		START_CRIT_SECTION();
		tempOff=PageAddItem(page, (Item) diskTuple, sizeof(int), InvalidOffsetNumber,false, false);
		tempOff=PageAddItem(page, (Item) (&hippoBuildState->sorted_list_pages), sizeof(BlockNumber), InvalidOffsetNumber,false, false);
		MarkBufferDirty(currentBuffer);
		END_CRIT_SECTION();
		LockBuffer(currentBuffer, BUFFER_LOCK_UNLOCK);

	}
	if(currentBlock<maxBlock+startBlock)
	{
		/*
		 *If this is not the last index page for storing sorted list, nothing special.
		 */
		iterationTimes=SORTED_LIST_TUPLES_PER_PAGE;
	}
	else
	{
		/*
		 * If this is the last index page for storing sorted list, this page will be not full.
		 */
		iterationTimes=remainder;
	}
	diskTuple=palloc(ItemPointerSize);
	/*
	 * Put index entries pointers in sorted list one by one.
	 */
	LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	for(i=0;i<iterationTimes;i++){
		int testOffsetNumber;
		len=0;
		serializeSortedListTuple(&(hippoBuildState->hippoItemPointer[(currentBlock-startBlock)*SORTED_LIST_TUPLES_PER_PAGE+i]),diskTuple);
		testOffsetNumber=PageAddItem(page, (Item) diskTuple, ItemPointerSize, InvalidOffsetNumber,false, false);
	}
	MarkBufferDirty(currentBuffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(currentBuffer);

}

/*
 * Insert a serialized index tuple into the index relation
 */
OffsetNumber
hippo_doinsert(HippoBuildState *buildstate)
{
	Page		page;
	OffsetNumber off,maxoff;
	Buffer buffer=buildstate->hp_currentInsertBuf;
	HippoTupleLong *memTuple=build_real_hippo_tuplelong(buildstate);
	Size itemsz;
	char *data=(char *)hippo_form_indextuple(memTuple,&itemsz,buildstate->histogramBoundsNum);
	/*
	 * Acquire a lock on buffer supplied by caller, if any.  If it doesn't have
	 * enough space, unpin it to obtain a new one below.
	 */
	if (BufferIsValid(buffer))
	{
		/*
		 *Check whether this working index page has enough space. If not, release pin and get a new one.
		 */
		if (PageGetFreeSpace(BufferGetPage(buffer)) < itemsz)
		{

			ReleaseBuffer(buffer);
			buffer = InvalidBuffer;
		}
	}
	else
	{
		elog(ERROR, "[hippo_doinsert] Buffer should not be invalid here. The caller should give a valid buffer.");
	}
	/*
	 * If we still don't have a usable buffer, let's get one buffer by ourselves.
	 */
	if (!BufferIsValid(buffer))
	{
		buffer = hippo_getinsertbuffer(buildstate->hp_irel);
		buildstate->hp_currentInsertBuf=buffer;
		if(!BufferIsValid(buffer))
		{
			/*
			 * This time we should definitely have one valid buffer. If not, show an error.
			 */
			elog(ERROR, "[hippo_doinsert] Try to get valid buffer but failed!");
		}
	}

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);
	/* Execute the actual insertion */
	START_CRIT_SECTION();
	buildstate->hippoItemPointer[buildstate->hp_indexnumtuples].blockNumber=BufferGetBlockNumber(buffer);
	off = PageAddItem(page, (Item) data, itemsz, InvalidOffsetNumber, false, false);
	buildstate->hippoItemPointer[buildstate->hp_indexnumtuples].ip_posid=off;
	maxoff=PageGetMaxOffsetNumber(page);
	if (off == InvalidOffsetNumber)
		{elog(ERROR, "[hippo_doinsert] could not insert new index tuple to page");}
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();

	/* Tuple is firmly on buffer; we can release our locks. But the pin is still there. */
	LockBuffer(buffer,BUFFER_LOCK_UNLOCK);
	/*
	 * Free serialized tuple
	 */
	pfree((char *)data);
	return off;
}

/*
 *  This function is to deserialize a single index entry pointer in sorted list when update Hippo.
 */
void deserializeSortedListTuple(HippoItemPointer *hippoItemPointer,char *diskTuple)
{
	int len=0;
	memcpy(&(hippoItemPointer->blockNumber),diskTuple,sizeof(uint32));
	len+=sizeof(uint32);
	memcpy(&(hippoItemPointer->ip_posid),diskTuple+len,sizeof(OffsetNumber));
	len+=sizeof(OffsetNumber);
}
/*
 *  This function is to serialize a single index entry pointer in sorted list when update Hippo.
 */
void serializeSortedListTuple(HippoItemPointer *hippoItemPointer,char *diskTuple)
{
	int len=0;
	memcpy(diskTuple,&(hippoItemPointer->blockNumber),sizeof(uint32));
	len+=sizeof(uint32);
	memcpy(diskTuple+len,&(hippoItemPointer->ip_posid),sizeof(OffsetNumber));
	len+=sizeof(OffsetNumber);
}

/*
 * Platform-dependent feature. Store the complete histogram in an area which can be managed by Hippo. This will speed up index update for data insertions.
 */
void put_histogram(Relation idxrel, BlockNumber startBlock, int histogramBoundsNum,Datum *histogramBounds)
{
	Buffer buffer;
	Page page;
	int totalNumber=histogramBoundsNum;
	int histogramIterator=0;
	int histogramBound;
	int maxBlock=histogramBoundsNum/HISTOGRAM_PER_PAGE;
	int i,j,off;
	for(i=startBlock;i<=startBlock+maxBlock;i++)
	{

	buffer=ReadBuffer(idxrel,i);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	if(i==startBlock){
	PageAddItem(page, (Item) (&totalNumber), sizeof(int), InvalidOffsetNumber,false, false);
	}
	for(j=0;j<HISTOGRAM_PER_PAGE;j++)
	{
		histogramIterator=i*HISTOGRAM_PER_PAGE+j;
		if(histogramIterator>=totalNumber)
		{
			break;
		}
		histogramBound=DatumGetInt32(histogramBounds[histogramIterator]);
		off=PageAddItem(page, (Item) (&histogramBound), sizeof(int), InvalidOffsetNumber,false, false);
	}
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
	}

}

/*
 * Platform-dependent feature. Retrieve the stored complete histogram bucket number from an area managed by Hippo.
 */
int get_histogram_totalNumber(Relation idxrel,BlockNumber startBlock)
{
	Buffer buffer;
	Page page;
	char* diskTuple;
	int totalNumber;
	buffer=ReadBuffer(idxrel,startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	diskTuple=(Item)PageGetItem(page,PageGetItemId(page,1));
	memcpy(&totalNumber,diskTuple,sizeof(int));
	UnlockReleaseBuffer(buffer);
	return totalNumber;
}

/*
 * Platform-dependent feature. Retrieve the stored complete histogram from an area managed by Hippo.
 */
int get_histogram(Relation idxrel,BlockNumber startblock,int histogramPosition)
{
	Buffer buffer;
	Page page;
	char* diskTuple;
	int value;
	BlockNumber blockNumber=histogramPosition/HISTOGRAM_PER_PAGE+startblock;
	Offset off=histogramPosition%HISTOGRAM_PER_PAGE+1;
	if(blockNumber==startblock)
	{
		off++;
	}
	buffer=ReadBuffer(idxrel,blockNumber);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	diskTuple=(Item)PageGetItem(page,PageGetItemId(page,off));
	memcpy(&value,diskTuple,sizeof(int));
	UnlockReleaseBuffer(buffer);
	return value;
}

/* Initialize a page */
void hippoinit_special(Buffer b)
{
	Page		page;
	Size		pageSize;
	pageSize = BufferGetPageSize(b);
	page = BufferGetPage(b);

	PageInit(page, pageSize, ItemPointerSize);
}

/*
 * Execute a binary search on the complete histogram stored on disk without loading them into memeory.
 */
void binary_search_histogram_ondisk(searchResult *histogramMatchData, Relation idxrel, Datum value,BlockNumber startBlock,int totalNumber)
{
	int histogramBoundsNum=totalNumber;
	int min=0,max=histogramBoundsNum-1,equalFlag,equalPosition,guess;
						histogramMatchData->index=-9999;
						histogramMatchData->numberOfGuesses=0;
						equalFlag=-1;
	int maxHistogramBound=get_histogram(idxrel,startBlock,max);
	int minHistogramBound=get_histogram(idxrel,startBlock,min);
						if(DatumGetInt32(value)>maxHistogramBound)
						{
							histogramMatchData->index=HISTOGRAM_OUT_OF_BOUNDARY;
							return;
						}
						if(DatumGetInt32(value)<minHistogramBound)
						{
							histogramMatchData->index=HISTOGRAM_OUT_OF_BOUNDARY;
							return;
						}
						while (min<=max) {

						        guess = (min + max) / 2;
						        histogramMatchData->numberOfGuesses++;
						        int result=get_histogram(idxrel,startBlock,guess);

						        if(result>DatumGetInt32(value))
						        {
						        	max=guess-1;
						        }
						        else
						        {
						        	min=guess+1;
						        }

						    }
						if(min==0)
						{
							histogramMatchData->index=0;
						}
						else
						{
						histogramMatchData->index=min-1;
						}
						return;
}

/*
 * Execute a binary search on the complete histogram stored in memory.
 */
void binary_search_histogram(searchResult *histogramMatchData,int histogramBoundsNum,Datum *histogramBounds, Datum value)
{
	int min=0,max=histogramBoundsNum-1,equalFlag,equalPosition,guess;
						histogramMatchData->index=-9999;
						histogramMatchData->numberOfGuesses=0;
						equalFlag=-1;
				        if((int)histogramBounds[min]==DatumGetInt32(value))
				        {
				        	equalFlag=min;

				        }
				        if((int)histogramBounds[max]==DatumGetInt32(value))
				        {
				        	equalFlag=max;

				        }

						while (min<=max) {

						        guess = (min + max) / 2;
						        histogramMatchData->numberOfGuesses++;
						        if((int)histogramBounds[guess]>DatumGetInt32(value))
						        {
						        	max=guess-1;
						        }
						        else
						        {
						        	min=guess+1;
						        }

						    }
						if(min==0)
						{
							histogramMatchData->index=0;
						}
						else
						{
						histogramMatchData->index=min-1;
						}
}


/*
 * Check whether Hippo can do a same page update.
 */
bool hippo_can_do_samepage_update(Buffer buffer, Size origsz, Size newsz)
{
	return
		((newsz <= origsz) ||
				PageGetFreeSpace(BufferGetPage(buffer)) >= (newsz - origsz));
}

/*
 * Update the index entry pointer in index entries sorted list when Hippo update occurs.
 */
void update_sorted_list_tuple(Relation idxrel,int index_tuple_id,BlockNumber diskBlock,OffsetNumber diskOffset,BlockNumber startBlock)
{
	int listBlock=index_tuple_id/SORTED_LIST_TUPLES_PER_PAGE;
	OffsetNumber listOffset=index_tuple_id%SORTED_LIST_TUPLES_PER_PAGE;
	Buffer buffer;
	Page page;
	OffsetNumber tempOff;
	HippoItemPointer hippoItemPointer;
	int len=0;
	char *sortedListDiskTuple=palloc(ItemPointerSize);
	hippoItemPointer.blockNumber=diskBlock;
	hippoItemPointer.ip_posid=diskOffset;
	buffer=ReadBuffer(idxrel,listBlock+startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	if((listBlock+startBlock)==startBlock)
	{
		listOffset+=3;
	}
	else
	{
		listOffset+=1;
	}
	PageIndexDeleteNoCompact(page,&listOffset,1);
	serializeSortedListTuple(&hippoItemPointer,sortedListDiskTuple);
	tempOff=PageAddItem(page, (Item) sortedListDiskTuple, ItemPointerSize, listOffset,true, false);
	pfree(sortedListDiskTuple);
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
}


/*
 * Check how many pages are occupied by index entries sorted list.
 */
void get_sorted_list_pages(Relation idxrel,BlockNumber *sorted_list_pages,BlockNumber startBlock)
{
	Buffer buffer=ReadBuffer(idxrel,startBlock);
	Page page=BufferGetPage(buffer);
	char* diskTuple;
	LockBuffer(buffer,BUFFER_LOCK_SHARE);
	diskTuple=(Item)PageGetItem(page,PageGetItemId(page,2));
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
	memcpy(sorted_list_pages,diskTuple,sizeof(BlockNumber));
}

/*
 * Add one new pointer to the index entries sorted list
 */
void add_new_sorted_list_tuple(Relation idxrel,int totalIndexTupleNumber,BlockNumber diskBlock,OffsetNumber diskOffset,BlockNumber startBlock)
{
	int listBlock=totalIndexTupleNumber/SORTED_LIST_TUPLES_PER_PAGE;
	Buffer buffer;
	Page page;
	HippoItemPointer hippoItemPointer;
	int len=0;
	char *sortedListDiskTuple=palloc(ItemPointerSize);
	hippoItemPointer.blockNumber=diskBlock;
	hippoItemPointer.ip_posid=diskOffset;
	buffer=ReadBuffer(idxrel,listBlock+startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	serializeSortedListTuple(&hippoItemPointer,sortedListDiskTuple);
	PageAddItem(page, (Item) sortedListDiskTuple, ItemPointerSize, InvalidOffsetNumber,false, false);
	pfree(sortedListDiskTuple);
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
	AddTotalIndexTupleNumber(idxrel,startBlock);
}

/*
 * Check whether one Hippo index entry summarizes the affected data pages
 * Return 1 means targetHeapBlock is larger than this position
 * Return -1 means targetHeapBlock is smaller than this position
 * Return 0 means targetHeapBlock belongs to this position
 */
int check_index_position(Relation idxrel,int index_tuple_id,BlockNumber targetHeapBlock,HippoTupleLong *hippoTupleLong,BlockNumber* diskBlock, OffsetNumber* diskOffset,BlockNumber startBlock)
{
	int listBlock=index_tuple_id/SORTED_LIST_TUPLES_PER_PAGE;
	OffsetNumber listOffset=index_tuple_id%SORTED_LIST_TUPLES_PER_PAGE;
	HippoItemPointer hippoItemPointer;
	Buffer buffer;
	Page page;
	Size itemsz;
	char* listDiskTuple;
	char* indexDiskTuple;
	buffer=ReadBuffer(idxrel,listBlock+startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	if(listBlock==0)
	{
		listDiskTuple=(Item)PageGetItem(page,PageGetItemId(page,listOffset+3));
	}
	else
	{
		listDiskTuple=(Item)PageGetItem(page,PageGetItemId(page,listOffset+1));
	}

	UnlockReleaseBuffer(buffer);

	deserializeSortedListTuple(&hippoItemPointer,listDiskTuple);
	/*
	 * Deserialize the BlockNumber
	 */
	*diskBlock=hippoItemPointer.blockNumber;
	*diskOffset=hippoItemPointer.ip_posid;
	buffer=ReadBuffer(idxrel,*diskBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	indexDiskTuple=(Item)PageGetItem(page,PageGetItemId(page,*diskOffset));
	UnlockReleaseBuffer(buffer);
	hippo_form_memtuple(hippoTupleLong,indexDiskTuple,&itemsz);
	if(targetHeapBlock>=hippoTupleLong->hp_PageStart&&targetHeapBlock<=hippoTupleLong->hp_PageNum)
	{
		return 0;
	}
	else if (targetHeapBlock<hippoTupleLong->hp_PageStart)
	{
		return -1;
	}
	else
	{
		return 1;
	}

}
/*
 * Return true means find this heap block. Return false means cannot find this heap block
 */
int GetTotalIndexTupleNumber(Relation idxrel,BlockNumber startBlock)
{
	Buffer buffer;
	Page page;
	char *listDiskTuple;
	int totalIndexTupleNumber;
	buffer=ReadBuffer(idxrel,startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	listDiskTuple=PageGetItem(page,PageGetItemId(page,1));
	memcpy(&totalIndexTupleNumber,listDiskTuple,sizeof(int));
	UnlockReleaseBuffer(buffer);
	return totalIndexTupleNumber;
}
void AddTotalIndexTupleNumber(Relation idxrel,BlockNumber startBlock)
{
	Buffer buffer;
	Page page;
	char *listDiskTuple;
	char *newListDiskTuple=palloc(sizeof(int));
	OffsetNumber FirstOffset=1;
	int totalIndexTupleNumber;
	buffer=ReadBuffer(idxrel,startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	listDiskTuple=PageGetItem(page,PageGetItemId(page,1));
	UnlockReleaseBuffer(buffer);
	memcpy(&totalIndexTupleNumber,listDiskTuple,sizeof(int));
	totalIndexTupleNumber+=1;
	memcpy(newListDiskTuple,&totalIndexTupleNumber,sizeof(int));
	buffer=ReadBuffer(idxrel,startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	PageIndexDeleteNoCompact(page,&FirstOffset,1);
	PageAddItem(page,(Item)newListDiskTuple,sizeof(int),1,true,false);
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
}

bool binary_search_sorted_list(Relation heaprel,Relation idxrel,int totalIndexTupleNumber,BlockNumber targetHeapBlock,int *resultPosition,HippoTupleLong *hippoTupleLong,BlockNumber* diskBlock, OffsetNumber* diskOffset,BlockNumber startBlock)
{
	int min=0,max=totalIndexTupleNumber-1,equalFlag,equalPosition,guess;
	int guessTimes=0;
	int checkResult;
	int i;
	equalFlag=-1;
	if(check_index_position(idxrel,max,targetHeapBlock,hippoTupleLong,diskBlock,diskOffset,startBlock)==0)
	{
		*resultPosition=max;
		return true;
	}
	else if(targetHeapBlock==(RelationGetNumberOfBlocks(heaprel)-1))
	{
		return false;
	}
	if(check_index_position(idxrel,min,targetHeapBlock,hippoTupleLong,diskBlock,diskOffset,startBlock)==0)
	{
		*resultPosition=min;
		return true;
	}
	while (max>=min) {
		guessTimes+=1;
		guess = (min + max) / 2;
		checkResult=check_index_position(idxrel,guess,targetHeapBlock,hippoTupleLong,diskBlock,diskOffset,startBlock);
		if(checkResult==0)
		{
			equalFlag=1;
			*resultPosition=guess;
			break;
		}
		if(checkResult==-1)
		{
			max=guess-1;
		}
		else
		{
			min=guess+1;
		}
	}
	if(equalFlag<0)
	{
		return false;
	}
	else
	{
		return true;
	}
}
/*
 * Form an index tuple. Deserialize the disk index entry.
 */
void hippo_form_memtuple(HippoTupleLong *hippoTupleLong,IndexTuple diskTuple,Size *memlen)
{

	char	   *ptr=(char *)diskTuple;
	char	    *diskGridSet;
	struct ewah_bitmap *compressedBitset;
	BlockNumber start;
	BlockNumber volume;
	int16 length;
	int			i;
	struct bitmap *originalBitset;
	int16 deleteFlag=0;
	*memlen = 0;
	compressedBitset=ewah_new();
	if(diskTuple==NULL)
	{
		elog(ERROR,"[hippo_form_memtuple] Transfer NULL diskTuple");
	}
	memcpy(&start,ptr,sizeof(BlockNumber));
	*memlen+=sizeof(BlockNumber);
	memcpy(&volume,ptr+*memlen,sizeof(BlockNumber));
	*memlen+=sizeof(BlockNumber);
	memcpy(&deleteFlag,ptr+*memlen,sizeof(int16));
	*memlen+=sizeof(int16);
	ewah_deserialize(compressedBitset,ptr+*memlen);
	hippoTupleLong->hp_PageStart=start;
	hippoTupleLong->hp_PageNum=volume;
	hippoTupleLong->deleteFlag=deleteFlag;
	hippoTupleLong->compressedBitset=compressedBitset;
	/*
	 * Decompress bitset
	 */
	originalBitset=ewah_to_bitmap(compressedBitset);
	hippoTupleLong->originalBitset=originalBitset;
}
/*
 * This function is to retrieve one Hippo index entry from disk
 */
void hippoGetNextIndexTuple(IndexScanDesc scan)
{
	Buffer		buffer;
	Page		page;
	Item diskTuple;
	HippoScanState *scanstate = (HippoScanState *) scan->opaque;
	OffsetNumber maxOffset;
	maxOffset=scanstate->maxOffset;
	scanstate->scanHasNext=false;
	if(scanstate->currentOffset<=maxOffset)
	{
		page=BufferGetPage(scanstate->currentLookupBuffer);
		diskTuple=PageGetItem(page,PageGetItemId(page,scanstate->currentOffset));
		if(diskTuple==NULL)
		{
			elog(ERROR,"[hippoGetNextIndexTuple] Got disktuple NULL");
		}
		scanstate->currentDiskTuple=diskTuple;
		scanstate->currentOffset++;
		scanstate->scanHasNext=true;
	}
	else{
	UnlockReleaseBuffer(scanstate->currentLookupBuffer);//, BUFFER_LOCK_UNLOCK);
	scanstate->currentLookupPageNum++;
	if(scanstate->currentLookupPageNum<scanstate->nBlocks)
	{
		buffer=ReadBuffer(scan->indexRelation,scanstate->currentLookupPageNum);
		LockBuffer(buffer,BUFFER_LOCK_SHARE);
		page=BufferGetPage(buffer);
		diskTuple=(IndexTuple)PageGetItem(page,PageGetItemId(page,1));
		if(diskTuple==NULL)
		{
			elog(ERROR,"[hippoGetNextIndexTuple] Got disktuple NULL");
		}
		scanstate->maxOffset=PageGetMaxOffsetNumber(page);
		scanstate->currentLookupBuffer=buffer;
		scanstate->currentOffset=2;
		scanstate->scanHasNext=true;
		scanstate->currentDiskTuple=diskTuple;
	}
	else
	{
		scanstate->scanHasNext=false;
	}
	}
	scan->opaque=scanstate;
}

