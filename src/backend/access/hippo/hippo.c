
#include "postgres.h"

#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/hippo.h"
#include "access/htup_details.h"
#include "access/xlogreader.h"

#include "catalog/index.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_index.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "catalog/pg_am.h"

#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"


#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/elog.h"
#include "utils/index_selfuncs.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "fmgr.h"
#include "ewok.h"

#define SORT_TYPE int16


/*
 * BRIN handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
hippohandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 5;
	amroutine->amsupport = 15;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = false;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = true;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = hippobuild;
	amroutine->ambuildempty = hippobuildempty;
	amroutine->aminsert = hippoinsert;
	amroutine->ambulkdelete = hippobulkdelete;
	amroutine->amvacuumcleanup = hippovacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = hippocostestimate;
	amroutine->amoptions = hippooptions;
	amroutine->amproperty = NULL;
	amroutine->amvalidate = NULL;
	amroutine->ambeginscan = hippobeginscan;
	amroutine->amrescan = hipporescan;
	amroutine->amgettuple = NULL;
	amroutine->amgetbitmap = hippogetbitmap;
	amroutine->amendscan = hippoendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * Per-data-tuple callback from IndexBuildHeapScan. You'd better not add log here otherwise the log file will have a crazy size.
 */
static void
hippobuildCallback(Relation index,
		HeapTuple htup,
		Datum *values,
		bool *isnull,
		bool tupleIsAlive,
		void *state)
{
	ereport(DEBUG2,(errmsg("[hippobuildCallback] start")));
	BlockNumber thisblock;
	HippoBuildState *buildstate = (HippoBuildState *) state;
	Datum *histogramBounds=buildstate->histogramBounds;
	int histogramBoundsNum=buildstate->histogramBoundsNum;
	thisblock = ItemPointerGetBlockNumber(&htup->t_self);
	ereport(DEBUG2,(errmsg("[hippobuildCallback] Retrieved necessary from buildstate")));
	ereport(DEBUG2,(errmsg("[hippobuildCallback] first histogram bound is %d",DatumGetInt32(histogramBounds[0]))));
	if(buildstate->hp_PageNum==0)
	{
		ereport(DEBUG2,(errmsg("[hippobuildCallback] Initialize the buildstate for first page")));
		/*
		 * We are working on the first index tuple.
		 */
		buildstate->hp_PageStart=thisblock;
		buildstate->hp_currentPage=thisblock;
		buildstate->hp_PageNum=thisblock;
		buildstate->hp_scanpage++;
		buildstate->hp_length=0;
		buildstate->dirtyFlag=true;

	}
	else
	{
		ereport(DEBUG2,(errmsg("[hippobuildCallback] Check the density")));
		/*
		 * For all incoming page, check current working partial histogram density.
		 */
		if(buildstate->hp_currentPage!=thisblock)
		{
			buildstate->hp_PageNum=thisblock;
			buildstate->dirtyFlag=true;
			buildstate->hp_scanpage++;
			if(buildstate->differentTuples*100>=buildstate->differenceThreshold*(buildstate->histogramBoundsNum-1))
			{
				/*
				 * Hippo density threshold is satisfied. We are going to put one index entry on disk. Note the Hippo SQL parameter is in percentage unit.
				 */
				buildstate->differentTuples=0;
				buildstate->stopMergeFlag=true;
				ereport(DEBUG2,(errmsg("[hippobuildCallback] The working index entry's density reaches the threshold.")));
			}
		}
	}
	/*
	 * Check whether the working partial histogram triggers the threshold. If so, put one index entry on disk.
	 */
	if(buildstate->stopMergeFlag==true)
	{
		ereport(DEBUG2,(errmsg("[hippobuildCallback] Stop merging and prepare to insert one index entry")));
		/*
		 * Extract information from buildstate to a new real hippo tuple long
		 */
		buildstate->stopMergeFlag=false;
		buildstate->deleteFlag=CLEAR_INDEX_TUPLE;
		/*
		 * Put one Hippo index entry on disk
		 */
		buildstate->hp_PageNum=buildstate->hp_PageNum-1;
		hippo_doinsert(buildstate);
		buildstate->hp_indexnumtuples++;
		buildstate->hp_PageStart=thisblock;
		buildstate->hp_PageNum=thisblock;
		buildstate->dirtyFlag=false;
		buildstate->hp_length=0;
//		buildstate->hp_grids[0]=-1;
		buildstate->originalBitset=bitmap_new();
		ereport(DEBUG2,(errmsg("[hippobuildCallback] Inserted one index entry")));
	}
	buildstate->hp_currentPage=thisblock;
	if (tupleIsAlive)
	{
		ereport(DEBUG2,(errmsg("[hippobuildCallback][Check a data tuple against the complete histogram] start")));
		/* Cost estimation info */
		buildstate->hp_numtuples++;
		/* Go through histogram one by one */
		searchResult histogramMatchData;
		/*
		 * Binary search histogram
		 */
		histogramMatchData.index=-9999;
		histogramMatchData.numberOfGuesses=0;
		binary_search_histogram(&histogramMatchData,histogramBoundsNum,histogramBounds, values[0]);
		/*
		if(histogramMatchData.index>histogramBoundsNum-1)
		{
			ereport(ERROR,(errmsg("[hippobuildCallback] Got an overflow tuple. Please rebuild the complete histogram by calling Analyze.")));
		}
		*/
		if(bitmap_get(buildstate->originalBitset,histogramMatchData.index)==false)
		{
			buildstate->differentTuples++;
			bitmap_set(buildstate->originalBitset,histogramMatchData.index);
		}
		ereport(DEBUG2,(errmsg("[hippobuildCallback][Check a data tuple against the complete histogram] stop")));
	}
	else
	{
		/*
		 *Got an deleted tuple. Don't care about it.
		 */
	}
	ereport(DEBUG2,(errmsg("[hippobuildCallback] stop")));
}

/*
 * Initialize a BrinBuildState appropriate to create tuples on the given index.
 */
static HippoBuildState *
initialize_hippo_buildstate(Relation heap, Relation index, Buffer buffer, AttrNumber attrNum, BlockNumber sorted_list_pages, Datum *histogramBounds,int histogramBoundsNum)
{
	ereport(DEBUG1,(errmsg("[initialize_hippo_buildstate] start")));
	HippoBuildState *buildstate;
	int iterator = 0;
	buildstate = palloc(sizeof(HippoBuildState));

	buildstate->hp_irel= index;
	buildstate->attrNum=attrNum;
	buildstate->hp_PageStart=0;
	buildstate->hp_PageNum=0;
	buildstate->dirtyFlag=false;
	buildstate->hp_currentPage=0;
	buildstate->hp_length=0;
	buildstate->hp_numtuples=0;
	buildstate->hp_indexnumtuples=0;
	buildstate->hp_scanpage=0;
	buildstate->hp_currentInsertBuf=buffer;
	//buildstate->hp_currentInsertPage=page;
	buildstate->lengthcounter=0;
	buildstate->histogramBounds=histogramBounds;
	buildstate->histogramBoundsNum=histogramBoundsNum;
	buildstate->originalBitset=bitmap_new();
	buildstate->pageBitmap=bitmap_new();
	buildstate->differenceThreshold=HippoGetMaxPagesPerRange(index);/* Hippo option: partial histogram density */
	buildstate->differentTuples=0;
	buildstate->stopMergeFlag=false;
	ereport(DEBUG1,(errmsg("[initialize_hippo_buildstate] start to initialize histogram bounds")));
	ereport(DEBUG1,(errmsg("[hippobuild] histogramBounds 3 is %d",DatumGetInt32(histogramBounds[2]))));
	/*
 	 for(iterator=0;iterator<histogramBoundsNum;iterator++)
		{
			buildstate->histogramBounds[iterator]=DatumGetInt32(histogramBounds[iterator]);
		}
	 */
	/*
	 * Initialize sorted list parameters
	 */
	buildstate->sorted_list_pages=sorted_list_pages;
	ereport(DEBUG1,(errmsg("[initialize_hippo_buildstate] stop")));
	return buildstate;
}
/*
 * Release resources associated with a HippoBuildState.
 */
static void
terminate_hippo_buildstate(HippoBuildState *buildstate)
{
	ereport(DEBUG1,(errmsg("[terminate_hippo_buildstate] start")));
	pfree(buildstate);
	ereport(DEBUG1,(errmsg("[terminate_hippo_buildstate] stop")));
}

void retrieve_histogram_stat(Relation heap, AttrNumber attrNum, Datum *histogramBounds, int *histogramBoundsNum)
{
	ereport(DEBUG1,(errmsg("[retrieve_histogram_stat] start")));
	/*
	 * Search PG kernel cache for pg_statistic info
	 */
	HeapTuple heapTuple=SearchSysCache2(STATRELATTINH,ObjectIdGetDatum(heap->rd_id),Int16GetDatum(attrNum));
	if (HeapTupleIsValid(heapTuple))
	{
		get_attstatsslot(heapTuple,
				get_atttype(heap->rd_id, attrNum),get_atttypmod(heap->rd_id,attrNum),
				STATISTIC_KIND_HISTOGRAM, InvalidOid,
				NULL,
				&histogramBounds, histogramBoundsNum,
				NULL, NULL);
	}
	/*
	 * If didn't release the stattuple, it will be locked.
	 */
	ReleaseSysCache(heapTuple);
	if(histogramBounds==NULL)
	{
		elog(ERROR, "[retrieve_histogram_stat] Got histogram NULL");
	}
	ereport(DEBUG1,(errmsg("[retrieve_histogram_stat] histogramBounds 3 is %d",(int)histogramBounds[2])));
	ereport(DEBUG1,(errmsg("[retrieve_histogram_stat] stop")));
}


Buffer initialize_hippo_space(Relation index, int histogramBoundsNum, int sorted_list_pages)
{
	ereport(DEBUG1,(errmsg("[initialize_hippo_space] start")));
	BlockNumber histogramPages=histogramBoundsNum/HISTOGRAM_PER_PAGE;
	Buffer buffer;
	Page page;/* Initial page */
	Size		pageSize;
	int i=0;
	/* Initialize pages for sorted list */
	for(i=0;i<sorted_list_pages+histogramPages+1;i++)
	{

		buffer=ReadBuffer(index,P_NEW);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		START_CRIT_SECTION();
		hippoinit_special(buffer);
		MarkBufferDirty(buffer);
		END_CRIT_SECTION();
		UnlockReleaseBuffer(buffer);
	}

	/*
	 *	Init HIPPO.
	 */
	buffer = ReadBuffer(index, P_NEW);

	if(buffer==NULL)
	{
		elog(ERROR, "[initialize_hippo_space] Initialized buffer NULL");
	}
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	pageSize = BufferGetPageSize(buffer);
	page = BufferGetPage(buffer);
	if(page==NULL)
	{
		elog(ERROR, "[initialize_hippo_space] Initialized page NULL");
	}
	PageInit(page, pageSize, sizeof(BlockNumber)*2+sizeof(GridList));
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	LockBuffer(buffer,BUFFER_LOCK_UNLOCK);
	ereport(DEBUG1,(errmsg("[initialize_hippo_space] stop")));
	return buffer;
}

/*
 *This function initializes the entire Hippo. It will call buildcallback many times.
 */
IndexBuildResult *hippobuild(Relation heap, Relation index, IndexInfo *indexInfo){
	ereport(DEBUG1,(errmsg("[hippobuild] start")));
	IndexBuildResult *result;
	double	reltuples;
	HippoBuildState *buildstate;
	Buffer		buffer;
	Datum *histogramBounds;
	int histogramBoundsNum;
	AttrNumber attrNum = indexInfo->ii_KeyAttrNumbers[0]; /* Current Hippo only support single column index */
	BlockNumber sorted_list_pages=((RelationGetNumberOfBlocks(heap)/((1)*SORTED_LIST_TUPLES_PER_PAGE))+1);

	//retrieve_histogram_stat(heap, attrNum, histogramBounds, &histogramBoundsNum);

	ereport(DEBUG1,(errmsg("[retrieve_histogram_stat] start")));
	/*
	 * Search PG kernel cache for pg_statistic info
	 */
	HeapTuple heapTuple=SearchSysCache2(STATRELATTINH,ObjectIdGetDatum(heap->rd_id),Int16GetDatum(attrNum));
	if (HeapTupleIsValid(heapTuple))
	{
		get_attstatsslot(heapTuple,
				get_atttype(heap->rd_id, attrNum),get_atttypmod(heap->rd_id,attrNum),
				STATISTIC_KIND_HISTOGRAM, InvalidOid,
				NULL,
				&histogramBounds, &histogramBoundsNum,
				NULL, NULL);
	}
	/*
	 * If didn't release the stattuple, it will be locked.
	 */
	ReleaseSysCache(heapTuple);
	if(histogramBounds==NULL)
	{
		elog(ERROR, "[retrieve_histogram_stat] Got histogram NULL");
	}
	ereport(DEBUG1,(errmsg("[retrieve_histogram_stat] stop")));

	buffer = initialize_hippo_space(index, histogramBoundsNum, sorted_list_pages);

	buildstate = initialize_hippo_buildstate(heap, index, buffer, attrNum, sorted_list_pages, histogramBounds, histogramBoundsNum);

	/* build the index */
	reltuples=IndexBuildHeapScan(heap, index, indexInfo, false, hippobuildCallback, (void *) buildstate);
	/*
	 * Finish the last index tuple.
	 */
	if(buildstate->dirtyFlag==true)
	{
		/*
		 * This is to summarize the last few data pages which are contained by buildstate but haven't got chances to be put on disk.
		 */
		buildstate->deleteFlag=buildstate->differentTuples;
		buildstate->hp_PageNum=buildstate->hp_currentPage;
		hippo_doinsert(buildstate);
		buildstate->hp_indexnumtuples++;
	}

	ReleaseBuffer(buildstate->hp_currentInsertBuf);
	put_histogram(index,0,histogramBoundsNum,histogramBounds);
	/*
	 * Stored sorted list
	 */
	SortedListInialize(index,buildstate,histogramBoundsNum/HISTOGRAM_PER_PAGE+1);

	terminate_hippo_buildstate(buildstate);
	/*
	 * Return statistics
	 */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	ereport(DEBUG1,(errmsg("[hippobuild] stop")));
	return result;
}
/*
 *	Pre-Build an empty HIPPO index in the initialization phase.
 */
void
hippobuildempty(Relation index)
{
	ereport(DEBUG1,(errmsg("[hippobuildempty] start")));
	int i;
	Buffer		buffer;
	/* Initialize pages for sorted list */
	for(i=0;i<SORTED_LIST_PAGES;i++)
	{
		buffer=ReadBuffer(index,P_NEW);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		START_CRIT_SECTION();
		hippoinit_special(buffer);
		MarkBufferDirty(buffer);
		END_CRIT_SECTION();
		UnlockReleaseBuffer(buffer);
	}
	ereport(DEBUG1,(errmsg("[hippobuildempty] stop")));
}



/*
 * HIPPO insert
 * A tuple in the heap is being inserted.  To keep a hippo index up to date,
 * we need to obtain the relevant index tuple and compare its stored values
 * with those of the new tuple.  If the tuple values are not consistent with
 * the summary tuple, we need to update the index tuple.
 *
 *
 */
bool
hippoinsert(Relation idxRel, Datum *values, bool *nulls,
		   ItemPointer heaptid, Relation heapRelation,
		   IndexUniqueCheck checkUnique)
{
	ereport(DEBUG1,(errmsg("[hippoinsert] start")));
	IndexInfo *indexInfo=BuildIndexInfo(idxRel);
	struct FormData_pg_index *heapInfo=idxRel->rd_index;
	/*
	 * Parameters defined by hippo itself
	 */
	BlockNumber heapBlk,totalblocks;
	Buffer currentBuffer,buffer;
	Page page;
	HippoTupleLong hippoTupleLong;
	AttrNumber attrNum=indexInfo->ii_KeyAttrNumbers[0];
	bool seekFlag=false;
	Datum *histogramBounds;
	BlockNumber indexDiskBlock;
	int histogramBoundsNum=get_histogram_totalNumber(idxRel,0);
	BlockNumber histogramPages=get_histogram_totalNumber(idxRel,0)/HISTOGRAM_PER_PAGE;
	OffsetNumber indexDiskOffset;
	int resultPosition;
	int totalIndexTupleNumber=GetTotalIndexTupleNumber(idxRel,histogramPages+1);


	MemoryContext tupcxt = NULL;
	MemoryContext oldcxt = NULL;

	tupcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "hippoinsert cxt",
								   ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(tupcxt);

	heapBlk = ItemPointerGetBlockNumber(heaptid);
	/*
	 * Binary search histogram
	*/
	searchResult histogramMatchData;
	binary_search_histogram_ondisk(&histogramMatchData,idxRel,values[0],0,histogramBoundsNum);
	/*
	 * This nested loop is for seeking the disk tuple which contains the heap tuple
	 */
	totalblocks=RelationGetNumberOfBlocks(idxRel);
	if(binary_search_sorted_list(heapRelation,idxRel,totalIndexTupleNumber,heapBlk,&resultPosition,&hippoTupleLong,&indexDiskBlock,&indexDiskOffset,histogramPages+1)==true)
	{
		seekFlag=true;
	}
	else
	{
		seekFlag=false;
		check_index_position(idxRel,totalIndexTupleNumber-1,heapBlk,&hippoTupleLong,&indexDiskBlock,&indexDiskOffset,histogramPages+1);
	}
	/*
	 * Update the memory tuple
	 */

	if(seekFlag==true){
		ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=true] start")));
		/*
		 * The inserted heap tuple belongs to one index tuple
		 */
		if(bitmap_get(hippoTupleLong.originalBitset,histogramMatchData.index)==false)
		{
			ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=true][update an index tuple] start")));
			/*
			 * Before update the memory tuple,copy the old memory tuple
			 */
			IndexTupleData *newDiskTuple;

			Size oldsize,newsize;
			bool samePageUpdate=true;
			HippoTupleLong newHippoTupleLong;
			oldsize = calculate_disk_indextuple_size(&hippoTupleLong);
			copy_hippo_mem_tuple(&newHippoTupleLong,&hippoTupleLong);
			bitmap_set(newHippoTupleLong.originalBitset,histogramMatchData.index);
			/*
			 *If hippo can do samepage update, go ahead and do it.If not, get new buffer, new page and insert them.
			 */
			newDiskTuple=hippo_form_indextuple(&newHippoTupleLong,&newsize);
			/*
			 * Check whether current index page has enough space. If so, go ahead and put it on disk.
			 * Otherwise, ask for new buffer and page.
			 */
			buffer=ReadBuffer(idxRel,indexDiskBlock);
			if(hippo_can_do_samepage_update(buffer,oldsize,newsize))
			{
				samePageUpdate=true;

			}
			else
			{
				samePageUpdate=false;
				ReleaseBuffer(buffer);
			}

			/*
			 *	Delete the old index tuple from disk
			 */

			if(samePageUpdate==true)
			{
				ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=true][update an index tuple][same page update] start")));
				/*
				 * Do samepage update
				 */
				OffsetNumber testOff;
				page=BufferGetPage(buffer);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

				START_CRIT_SECTION();
				PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
				testOff=PageAddItem(page,(Item)newDiskTuple,newsize,indexDiskOffset,true,false);
				if(testOff==InvalidOffsetNumber)
				{
					elog(ERROR, "[hippoinsert] failed to add HIPPO tuple same page");
				}
				MarkBufferDirty(buffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(buffer);
				ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=true][update an index tuple][same page update] stop")));
			}
			else
			{
				ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=true][update an index tuple][different page update] start")));
				/*
				 * Create a new page and insert disk tuple
				 */
				Buffer newInsertBuffer;
				Page newInsertPage;
				BlockNumber newBlockNumber;
				OffsetNumber newOffsetNumber;
				if(indexDiskBlock<totalblocks-1){
				currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
				page=BufferGetPage(currentBuffer);
				LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
				START_CRIT_SECTION();
				PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
				MarkBufferDirty(currentBuffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(currentBuffer);
				newInsertBuffer=ReadBuffer(idxRel,totalblocks-1);
				newInsertPage = BufferGetPage(newInsertBuffer);
				if(PageGetFreeSpace(newInsertPage)<newsize)
				{
					ReleaseBuffer(newInsertBuffer);
					newInsertBuffer=ReadBuffer(idxRel,totalblocks);
					newInsertPage = BufferGetPage(newInsertBuffer);

				}
				newBlockNumber=BufferGetBlockNumber(newInsertBuffer);
				LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
				START_CRIT_SECTION();
				newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,false, false);
				if(newOffsetNumber==InvalidOffsetNumber)
				{
					elog(ERROR, "failed to add HIPPO tuple new page");
				}
				MarkBufferDirty(newInsertBuffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(newInsertBuffer);
				update_sorted_list_tuple(idxRel,resultPosition,newBlockNumber,newOffsetNumber,histogramPages+1);
				}
				else
				{
					currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
					page=BufferGetPage(currentBuffer);
					LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
					MarkBufferDirty(currentBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(currentBuffer);
					hippo_getinsertbuffer(idxRel);
					newInsertBuffer=ReadBuffer(idxRel,totalblocks);
					newBlockNumber=totalblocks;
					newInsertPage = BufferGetPage(newInsertBuffer);
					LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,
											  false, false);
					if(newOffsetNumber==InvalidOffsetNumber)
					{
						elog(ERROR, "[hippoinsert]failed to add HIPPO tuple new page");
					}
					MarkBufferDirty(newInsertBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(newInsertBuffer);
					update_sorted_list_tuple(idxRel,resultPosition,newBlockNumber,newOffsetNumber,histogramPages+1);
				}
				ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=true][update an index tuple][different page update] stop")));
			}
			ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=true][update an index tuple] stop")));
		}
		else
		{
			ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=true][no need to update an index tuple] do nothing")));
			/*
			 * The new inserted value doesn't change the original index tuple, thus do nothing.
			 */
		}
		ewah_free(hippoTupleLong.compressedBitset);
		ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=true] stop")));
	}
	else
	{
		ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false] start")));
		/*
		 * The inserted heap tuple doesn't belong to one index tuple. First check whether the
		 * last index (the index contains the last heap page) is full. If full, create a new index tuple. If not full, go ahead and merge
		 * its page into this tuple.
		 */
		/*
		 *Check whether this tuple is full. If full, ask for new buffer and page. If not,delete the old one and insert new one.
		 */
		if((hippoTupleLong.deleteFlag*1.0/histogramBoundsNum-1)>=(HippoGetMaxPagesPerRange(idxRel)*1.00/100))
		{
			ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false][create new last index entry] start")));
			/*
			 * Full. Keep the last index tuple there and create new buffer, page and last index tuple.
			 */
			HippoTupleLong newLastIndexTuple;
			char *newLastIndexDiskTuple;
			Size itemsize;
			Buffer newInsertBuffer;
			Page newInsertPage;
			BlockNumber newBlock;
			OffsetNumber newOffsetNumber;
			newLastIndexTuple.hp_PageStart=heapBlk;
			newLastIndexTuple.hp_PageNum=heapBlk;
			newLastIndexTuple.deleteFlag=1;
			newLastIndexTuple.originalBitset=bitmap_new();
			bitmap_set(newLastIndexTuple.originalBitset,histogramMatchData.index);
			newLastIndexDiskTuple=hippo_form_indextuple(&newLastIndexTuple,&itemsize);
			newInsertBuffer=ReadBuffer(idxRel,totalblocks-1);
			newInsertPage=BufferGetPage(newInsertBuffer);
			if(PageGetFreeSpace(newInsertPage)<itemsize)
			{
				ReleaseBuffer(newInsertBuffer);
				hippo_getinsertbuffer(idxRel);
				newInsertBuffer=ReadBuffer(idxRel,totalblocks);
				newInsertPage=BufferGetPage(newInsertBuffer);
			}
			newBlock=BufferGetBlockNumber(newInsertBuffer);
			LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
			START_CRIT_SECTION();
			newOffsetNumber=PageAddItem(newInsertPage,(Item)newLastIndexDiskTuple,itemsize,InvalidOffsetNumber,false,false);
			if(newOffsetNumber==InvalidOffsetNumber)
			{
				elog(ERROR, "failed to update HIPPO last index tuple same page");
			}
			MarkBufferDirty(newInsertBuffer);
			END_CRIT_SECTION();
			UnlockReleaseBuffer(newInsertBuffer);
			add_new_sorted_list_tuple(idxRel,totalIndexTupleNumber,newBlock,newOffsetNumber,histogramPages+1);
			ewah_free(hippoTupleLong.compressedBitset);
			ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false][create new last index entry] stop")));
		}
		else
		{
			ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false][update current last index entry] start")));
			/*
			 * Not full. Update last index tuple. Check whether we can do samepage insert. If not, ask for new buffer and page.
			 */
			IndexTupleData *newDiskTuple;
			Size oldsize,newsize;
			HippoTupleLong newLastHippoTupleLong;
			bool samePageUpdate=true;
			oldsize = calculate_disk_indextuple_size(&hippoTupleLong);
			copy_hippo_mem_tuple(&newLastHippoTupleLong,&hippoTupleLong);
			newLastHippoTupleLong.hp_PageNum=heapBlk;;
			newLastHippoTupleLong.deleteFlag++;
			bitmap_set(newLastHippoTupleLong.originalBitset,histogramMatchData.index);
			newDiskTuple=hippo_form_indextuple(&newLastHippoTupleLong,&newsize);;
			currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
			page=BufferGetPage(currentBuffer);
			if(hippo_can_do_samepage_update(currentBuffer,oldsize,newsize))
			{
				samePageUpdate=true;
			}
			else
			{
				samePageUpdate=false;
				ReleaseBuffer(currentBuffer);
			}
			/*
			 *	Delete the old index tuple from disk
			 */
			if(samePageUpdate==true)
			{
				ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false][update current last index entry][same page update] start")));
				/*
				 * Do samepage update
				 */
				LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
				START_CRIT_SECTION();
				PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
				if(PageAddItem(page,(Item)newDiskTuple,newsize,indexDiskOffset,true,false)==InvalidOffsetNumber)
				{
					elog(ERROR, "failed to add HIPPO last index tuple same page");
				}
				MarkBufferDirty(currentBuffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(currentBuffer);
				ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false][update current last index entry][same page update] stop")));
			}
			else
			{
				ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false][update current last index entry][new page update] start")));
				/*
				 * Create a new page and insert disk tuple
				 */
				Buffer newInsertBuffer;
				Page newInsertPage;
				BlockNumber newBlockNumber;
				OffsetNumber newOffsetNumber;

				if(indexDiskBlock<totalblocks-1){
					currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
					page=BufferGetPage(currentBuffer);
					LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
					MarkBufferDirty(currentBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(currentBuffer);
					newInsertBuffer=ReadBuffer(idxRel,totalblocks-1);
					if(PageGetFreeSpace(newInsertPage)<newsize)
					{
						ReleaseBuffer(newInsertBuffer);
						newInsertBuffer=hippo_getinsertbuffer(idxRel);
						newInsertPage = BufferGetPage(newInsertBuffer);
					}
					newBlockNumber=BufferGetBlockNumber(newInsertBuffer);
					LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,
												  false, false);
					MarkBufferDirty(newInsertBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(newInsertBuffer);
				}
				else
				{
					currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
					page=BufferGetPage(currentBuffer);
					LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);

					START_CRIT_SECTION();
					PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
					MarkBufferDirty(currentBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(currentBuffer);
					newInsertBuffer=hippo_getinsertbuffer(idxRel);
					newInsertPage = BufferGetPage(newInsertBuffer);
					newBlockNumber=BufferGetBlockNumber(newInsertBuffer);
					LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,
							  false, false);
					MarkBufferDirty(newInsertBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(newInsertBuffer);
				}
				update_sorted_list_tuple(idxRel,totalIndexTupleNumber-1,newBlockNumber,newOffsetNumber,histogramPages+1);
				ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false][update current last index entry][new page update] stop")));
			}
			ewah_free(hippoTupleLong.compressedBitset);
			ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false][update current last index entry] start")));
		}
		ereport(DEBUG1,(errmsg("[hippoinsert][seekFlag=false] stop")));

	}
	/*
	 *	Update the disk tuple
	 */
	//free_attstatsslot(get_atttype(heapInfo->indrelid, attrNum),histogramBounds,histogramBoundsNum,NULL,0);
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tupcxt);
	ereport(DEBUG1,(errmsg("[hippoinsert] stop")));
	return false;
}


/*
 * hippobulkdelete
 *	HIPPO will update itself after each deletion. Note that: the deletion here is not per-tuple deletion. In this deletion, Postgres may delete millions of records in one time.
 * we could mark item tuples as "dirty" (when a minimum or maximum heap
 * tuple is deleted), meaning the need to re-run summarization on the affected
 * range.  Would need to add an extra flag in hippotuple for that.
 */
IndexBulkDeleteResult *
hippobulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
		   IndexBulkDeleteCallback callback, void *callback_state)
{
	ereport(DEBUG1,(errmsg("[hippobulkdelete] start")));
	BlockNumber startblock;
	BlockNumber sorted_list_pages;
	BlockNumber histogramPages;
	BlockNumber totalHeapBlocks;
	/* allocate stats if first time through, else re-use existing struct */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));


	BlockNumber idxBlkNum=0,heapBlkNum=0;
	Relation idxRelation,heapRelation;
	Oid heapRelationOid;
	int histogramBoundsNum;
	Datum *histogramBounds;
	IndexInfo *indexInfo=BuildIndexInfo(info->index);
	AttrNumber attrNum=indexInfo->ii_KeyAttrNumbers[0];
	HeapTuple histogramTuple;
	idxRelation=info->index;
	heapRelationOid=IndexGetRelation(RelationGetRelid(idxRelation), false);
	heapRelation=relation_open(heapRelationOid, AccessShareLock);
	totalHeapBlocks=RelationGetNumberOfBlocks(heapRelation);
	struct FormData_pg_index *heapInfo=idxRelation->rd_index;
	/*
	 *Pre-retrieve histogram from PG kernel
	 */
	histogramTuple=SearchSysCache2(STATRELATTINH,ObjectIdGetDatum(heapInfo->indrelid),Int16GetDatum(attrNum));
	if (HeapTupleIsValid(histogramTuple))
	{
		get_attstatsslot(histogramTuple,
				get_atttype(heapInfo->indrelid, attrNum),get_atttypmod(heapInfo->indrelid,attrNum),
						STATISTIC_KIND_HISTOGRAM, InvalidOid,
						NULL,
						&histogramBounds, &histogramBoundsNum,
						NULL, NULL);
	}
	ReleaseSysCache(histogramTuple);
/*
 * Set the start block to skip the histogram and sorted list
 */
	histogramPages=histogramBoundsNum/HISTOGRAM_PER_PAGE;
	get_sorted_list_pages(idxRelation,&sorted_list_pages,histogramPages+1);
	startblock=sorted_list_pages+histogramPages+1;
/*
 * First loop, traverse each index page
 */
	for(idxBlkNum=startblock;idxBlkNum<RelationGetNumberOfBlocks(idxRelation);idxBlkNum++)
	{

		Buffer currentBuffer=ReadBuffer(idxRelation,idxBlkNum);
		Page currentPage=BufferGetPage(currentBuffer);
		OffsetNumber idxTupleOffset,maxOffset;
		HippoTupleLong hippoTupleLong;
		bool indexDeleteFlag;
		/*
		 *Acquire an exclusive lock in case execute in place index entry update
		 */
		LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
		maxOffset=PageGetMaxOffsetNumber(currentPage);
		Size itemsz;
		IndexTuple currentDiskTuple;
		/*
		 * Second loop, traverse the each index entry inside each index page
		 */
		for(idxTupleOffset=1;idxTupleOffset<=maxOffset;idxTupleOffset++)
		{

			currentDiskTuple=(IndexTuple)PageGetItem(currentPage,PageGetItemId(currentPage,idxTupleOffset));
			hippo_form_memtuple(&hippoTupleLong,currentDiskTuple,&itemsz);
			indexDeleteFlag=false;
			/*
			 * The index tuple will be checked
			 */
			for(heapBlkNum=hippoTupleLong.hp_PageStart;heapBlkNum<=hippoTupleLong.hp_PageNum;heapBlkNum++)
			{
				/*
				 * Check each data page summarized by this index entry
				 */
				Buffer currentHeapBuffer=ReadBuffer(heapRelation,heapBlkNum);
				Page currentHeapPage=BufferGetPage(currentHeapBuffer);
				Size recordedFreeSpace=GetRecordedFreeSpace(heapRelation,heapBlkNum);
				Size pageFreeSpace=PageGetFreeSpace(currentHeapPage);
				if(pageFreeSpace-recordedFreeSpace>24&&recordedFreeSpace==0)
				{
					/*
					 * Hippo detects some data tuples are deleted. It will break the loop and start to re-summarize.
					 */
					indexDeleteFlag=true;
					ReleaseBuffer(currentHeapBuffer);
					break;
				}
				else{
					/*
					 * Hippo doesn't find anything deleted on this data page. Scan the next data page.
					 */
					ReleaseBuffer(currentHeapBuffer);}
			}
			if(indexDeleteFlag==true)
			{
			/*
			 * DeleteFalg is true. Hippo detects something deleted. This index tuple should be updated.
			 */

				bitmap_free(hippoTupleLong.originalBitset);
				hippoTupleLong.originalBitset=bitmap_new();
				Size diskSize;
			for(heapBlkNum=hippoTupleLong.hp_PageStart;heapBlkNum<=hippoTupleLong.hp_PageNum;heapBlkNum++)
			{
				/*
				 * Traverse each data page summarized by this index entry
				 */
				Buffer currentHeapBuffer=ReadBuffer(heapRelation,heapBlkNum);
				Page currentHeapPage=BufferGetPage(currentHeapBuffer);
				ItemId lp;
				ItemPointerData itmPtr;

				OffsetNumber maxOffset=PageGetMaxOffsetNumber(currentHeapPage),heapTupleOffset;
				Datum value;
				bool isnull;//=false;
				LockBuffer(currentHeapBuffer, BUFFER_LOCK_SHARE);
				for(heapTupleOffset=1;heapTupleOffset<=maxOffset;heapTupleOffset++)
				{
					/*
					 *Traverse each data tuple belongs to this data page
					 */
					lp = PageGetItemId(currentHeapPage,heapTupleOffset);
					if(lp->lp_len==0)
					{
						ereport(DEBUG5,(errmsg("[hippobulkdelete][Iterate data tuple on each parent page] Got an empty data tuple.")));
						continue;
					}
					ItemPointerSet(&itmPtr, heapBlkNum, heapTupleOffset);
					HeapTupleData currentHeapTuple;
					currentHeapTuple.t_data=(HeapTupleHeader)PageGetItem(currentHeapPage,lp);
					currentHeapTuple.t_len=ItemIdGetLength(lp);
					currentHeapTuple.t_tableOid = RelationGetRelid(heapRelation);
					currentHeapTuple.t_self=itmPtr;
					if(!HeapTupleIsValid(&currentHeapTuple))
					{
						elog(LOG,"Got NULL heap tuple");
					}

					value=fastgetattr(&currentHeapTuple,attrNum,RelationGetDescr(heapRelation),&isnull);
					searchResult histogramMatchData;
					binary_search_histogram(&histogramMatchData,histogramBoundsNum,histogramBounds,value);
					bitmap_set(hippoTupleLong.originalBitset,histogramMatchData.index);
				}
				UnlockReleaseBuffer(currentHeapBuffer);
			}
			/*
			 * Update the index tuple on disk
							 */
				IndexTupleData *diskTuple;
				diskTuple=hippo_form_indextuple(&hippoTupleLong,&diskSize);
				START_CRIT_SECTION();
				/*
				 *Delete the old index entry
				 */
				PageIndexDeleteNoCompact(currentPage,&idxTupleOffset,1);
				/*
				 *Add the new index entry
				 */
				if(PageAddItem(currentPage,(Item)diskTuple,diskSize,idxTupleOffset,true,false)==InvalidOffsetNumber)
				{
					elog(ERROR, "Failed to add HIPPO last index tuple same page");
				}
				MarkBufferDirty(currentBuffer);
				END_CRIT_SECTION();

				pfree(diskTuple);
				pfree(hippoTupleLong.originalBitset);
				pfree(hippoTupleLong.compressedBitset);
				//currentBuffer=ReadBuffer(idxRelation,idxBlkNum);
				//LockBuffer(currentBuffer, BUFFER_LOCK_SHARE);
			}
			else
			{
				/*
				 *This index entry does not need to re-summarize.
				 */
				pfree(hippoTupleLong.originalBitset);
				pfree(hippoTupleLong.compressedBitset);
				//UnlockReleaseBuffer(currentBuffer);
			}

		}

		UnlockReleaseBuffer(currentBuffer);
	}
	relation_close(heapRelation, AccessShareLock);
	ereport(DEBUG1,(errmsg("[hippobulkdelete] stop")));
	return stats;
}

/*
 * This routine is in charge of "vacuuming" a HIPPO index. It doessome statistics for PG.
 */
IndexBulkDeleteResult *
hippovacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	ereport(DEBUG1,(errmsg("[hippovacuumcleanup] do nothing")));
	return stats;
}

IndexScanDesc
hippobeginscan(Relation r, int nkeys, int norderbys)
{
	ereport(DEBUG1,(errmsg("[hippobeginscan] do nothing")));
	IndexScanDesc scan = RelationGetIndexScan(r, nkeys, norderbys);
	return scan;
}


/*
 * This function does index search.
 */
int64 hippogetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	ereport(DEBUG1,(errmsg("[hippogetbitmap] start")));
	Relation	idxRel = scan->indexRelation;
	HippoTupleLong hippoTupleLong;
	Oid			heapOid;
	Relation	heapRel;
	HippoScanState scanstate;//=(HippoScanState *) scan->opaque;
	BlockNumber sorted_list_pages;

	BlockNumber nblocks;
	Size itemsz;
	IndexInfo *indexInfo;
	//int			totalpages = 0;
	bool matchFlag=false;
	bool scankeymatchflag=false;
	int totalPages=0;
	int i,j,counter,k;
	int histogramBoundsNum;
	Datum *histogramBounds;
	ScanKey keys=scan->keyData;
	HeapTuple heapTuple; /* pg_statistic data */
	Page page;
	IndexTuple diskTuple;
	BlockNumber histogramPages;
	/*
	 * Compressed query bitmap
	 */
	struct ewah_bitmap *compressedBitset;
	struct bitmap *gridBitset;
	/*
	 * Store qualified grids
	 */
	int predicateLen=0;
	int predicateGrids[10000];
	Size queryItemsz;
	struct ewah_iterator	scankeyIterator;
	eword_t nextWordScankey;
	eword_t nextWordTuple;
	scan->opaque=&scanstate;
	/*
	 * Currently HIPPO supports the numerOfKeys is no larger than 1
	 */
	int nkeys=scan->numberOfKeys;
	AttrNumber attrNum;
	struct FormData_pg_index *heapInfo=idxRel->rd_index;
	indexInfo=BuildIndexInfo(idxRel);
	scan->opaque=&scanstate;
	scanstate.length=0;
	attrNum=indexInfo->ii_KeyAttrNumbers[0];
	if(heapInfo==NULL)
	{
		elog(ERROR, "[hippogetbitmap] Heap is null");
	}
	histogramBoundsNum=get_histogram_totalNumber(idxRel,0);
	scanstate.histogramBoundsNum=histogramBoundsNum;
	histogramPages=histogramBoundsNum/HISTOGRAM_PER_PAGE;
	get_sorted_list_pages(idxRel,&sorted_list_pages,histogramPages+1);
	scanstate.currentLookupBuffer=ReadBuffer(scan->indexRelation,sorted_list_pages+histogramPages+1);
	if(scanstate.currentLookupBuffer==NULL)
	{
		elog(ERROR,"[hippogetbitmap] Read NULL buffer");
	}
	LockBuffer(scanstate.currentLookupBuffer, BUFFER_LOCK_SHARE);
	page=BufferGetPage(scanstate.currentLookupBuffer);
	if(page==NULL)
	{
		elog(ERROR,"[hippogetbitmap] Read NULL page");
	}
	diskTuple=(IndexTuple)PageGetItem(page,PageGetItemId(page,1));
	scanstate.maxOffset=PageGetMaxOffsetNumber(page);
	scanstate.currentLookupPageNum=sorted_list_pages+histogramPages+1;
	scanstate.currentOffset=1;
	scanstate.currentDiskTuple=diskTuple;
	scanstate.length=0;
	scanstate.scanHasNext=true;
	bool test[nkeys][histogramBoundsNum];
	bool gridtest[nkeys][histogramBoundsNum];

	/*
	 * Check whether one histogram bound satisfies scankeys. It is true only under this circumstance that this bound satisfies all the keys.
	 */
	/*
	 * Iterate scan key to find matched grids
	 */
	for(k=0;k<nkeys;k++){
	if(keys[k].sk_strategy!=3){
		ereport(DEBUG1,(errmsg("[hippogetbitmap][handle inequality operator] start")));
		/*
		 * Handle all the operators <, >, <=, >=, except =
		 */
			searchResult histogramMatchData;
			binary_search_histogram_ondisk(&histogramMatchData,idxRel,DatumGetInt32(keys[k].sk_argument),0,histogramBoundsNum);
			if(histogramMatchData.index==HISTOGRAM_OUT_OF_BOUNDARY)
			{
				for(i=0;i<=histogramBoundsNum-1;i++)
				{
					gridtest[k][i]=false;
				}
			}
			else
			{
			if(keys[k].sk_strategy==1 || keys[k].sk_strategy==2)
			{
				for(i=0;i<=histogramMatchData.index;i++)
				{
					gridtest[k][i]=true;
				}
				for(i=histogramMatchData.index+1;i<=histogramBoundsNum-1;i++)
				{
					gridtest[k][i]=false;
				}

			}
			else if(keys[k].sk_strategy==5 || keys[k].sk_strategy==4)
			{
				for(i=0;i<histogramMatchData.index;i++)
				{
					gridtest[k][i]=false;
				}
				for(i=histogramMatchData.index;i<=histogramBoundsNum-1;i++)
				{
					gridtest[k][i]=true;
				}
			}
			}
			ereport(DEBUG1,(errmsg("[hippogetbitmap][handle inequality operator] stop")));
	}
	else
	{
		ereport(DEBUG1,(errmsg("[hippogetbitmap][handle equality operator] start")));
		/*
		 * Handle the case that strategy number is 3 which means "equal" like id = 100
		 */
		int data=DatumGetInt32(keys[k].sk_argument);
		searchResult histogramMatchData;
		binary_search_histogram_ondisk(&histogramMatchData,idxRel,DatumGetInt32(keys[k].sk_argument),0,histogramBoundsNum);
		gridtest[k][histogramMatchData.index]=true;
		for(i=0;i<=histogramBoundsNum-1;i++)
		{
			gridtest[k][i]=false;
		}
		if(histogramMatchData.index!=HISTOGRAM_OUT_OF_BOUNDARY)
		{
			gridtest[k][histogramMatchData.index]=true;
		}
		ereport(DEBUG1,(errmsg("[hippogetbitmap][handle equality operator] stop")));
	}
	}
	gridBitset=bitmap_new();
	for(i=0;i<=histogramBoundsNum-1;i++)
	{
		scankeymatchflag=gridtest[0][i];
		for(k=0;k<nkeys;k++)
		{
			scankeymatchflag=scankeymatchflag&&gridtest[k][i];
		}
		if(scankeymatchflag==true)
		{
			predicateGrids[predicateLen]=i;
			predicateLen++;
			bitmap_set(gridBitset,i);
		}
		ereport(DEBUG1,(errmsg("[hippogetbitmap]Got the partial histogram of query predicate")));
	}
	/*
	 * We need to know the size of the table so that we know how long to
	 * iterate on the hippo.
	 */
	heapOid =idxRel->rd_id;
	heapRel = index_open(heapOid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(idxRel);
	scanstate.nBlocks=nblocks;
	index_close(heapRel, AccessShareLock);
	hippoGetNextIndexTuple(scan);
	do{
		ereport(DEBUG1,(errmsg("[hippogetbitmap]Got the partial histogram of query predicate")));
		hippoTupleLong.hp_PageStart=0;
		hippoTupleLong.hp_PageNum=0;
		hippoTupleLong.length=0;
		hippo_form_memtuple(&hippoTupleLong,scanstate.currentDiskTuple,&itemsz);
		matchFlag=false;
		/*
		 * Traverse two bitmaps (one from index entry and one from query predicate) to find one match. If so, break the loop.
		 */
		for(i=0;i<predicateLen;i++)
		{
			if(bitmap_get(hippoTupleLong.originalBitset,predicateGrids[i])==true)
			{
				for(j=hippoTupleLong.hp_PageStart;j<=hippoTupleLong.hp_PageNum;j++)
				{
					tbm_add_page(tbm,j);
					totalPages++;
				}
				break;
			}
		}
		ewah_free(hippoTupleLong.compressedBitset);
		bitmap_free(hippoTupleLong.originalBitset);
		hippoGetNextIndexTuple(scan);
		counter++;
		}
	while(scanstate.scanHasNext==true);
	ereport(DEBUG1,(errmsg("[hippogetbitmap] stop")));
	return (totalPages * 10);
}


void
hippoendscan(IndexScanDesc scan)
{
	ereport(DEBUG1,(errmsg("[hippoendscan] do nothing")));
}

void
hippo_redo(XLogReaderState *record)
{
	ereport(DEBUG1,(errmsg("[hippo_redo] do nothing")));
}
/*
 * Re-initialize state for a HIPPO index scan
 */
void
hipporescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		   ScanKey orderbys, int norderbys)
{
	ereport(DEBUG1,(errmsg("[hipporescan] start")));
	/*
	 * Other index AMs preprocess the scan keys at this point, or sometime
	 * early during the scan; this lets them optimize by removing redundant
	 * keys, or doing early returns when they are impossible to satisfy; see
	 * _bt_preprocess_keys for an example.  Something like that could be added
	 * here someday, too.
	 */

	if (scankey && scan->numberOfKeys > 0){
		memmove(scan->keyData, scankey,scan->numberOfKeys * sizeof(ScanKeyData));}

	ereport(DEBUG1,(errmsg("[hipporescan] stop")));
}
/*
 * reloptions processor for HIPPO indexes
 */
bytea *
hippooptions(Datum reloptions, bool validate)
{
	ereport(DEBUG1,(errmsg("[hippooptions] start")));
	relopt_value *options;
	HippoOptions *rdopts;
	int			numoptions;
	static const relopt_parse_elt tab[] = {
		{"density", RELOPT_TYPE_INT, offsetof(HippoOptions, density)}
	};

	options = parseRelOptions(reloptions, validate, RELOPT_KIND_HIPPO,
							  &numoptions);

	/* if none set, we're done */
	if (numoptions == 0)
		return NULL;

	rdopts = allocateReloptStruct(sizeof(HippoOptions), options, numoptions);

	fillRelOptions((void *) rdopts, sizeof(HippoOptions), options, numoptions,
				   validate, tab, lengthof(tab));

	pfree(options);
	ereport(DEBUG1,(errmsg("[hippooptions] stop")));
	return (bytea *) rdopts;
}

