#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <stdlib.h>
#include <stdio.h>
#include <cstring>

/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The right attribute in the join predicate (INDEXED)
                      const int reclen)               // Length of a tuple in the output relation
{
  	cout << "Algorithm: Indexed NL Join" << endl;

	AttrDesc* r;
	int num;
	Status status = attrCat->getRelInfo(result, num, r);
	if(status != OK) return status;

	// Load heap 1
	HeapFileScan heap1 = HeapFileScan(attrDesc1.relName, status);
	if (status != OK) return status; 
	
	// Load heap 2
	HeapFileScan heap2 = HeapFileScan(attrDesc2.relName, status);
	if (status != OK) return status;
	
	// Open output heap
	HeapFile heapOut = HeapFile(result, status);
	if (status != OK) return status; 

	// Get size of output record
	int size = 0;
	for (int x = 0; x < projCnt; x++) 
	{
		size += attrDescArray[x].attrLen;
	}

	// Create/find the index (checked to ensure indexed before entering this fn)
	Index index = Index(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, (Datatype)attrDesc2.attrType, 1, status);
    	if (status != OK) return status;

	//Start Join 
	RID rid, rid1, rid2;
	Record record1, record2;

	while(heap1.scanNext(rid1, record1) == OK)
	{
		char* data1 = (char *) record1.data;

		// Update Heapfile Scan for Next Loop
		heap2 = HeapFileScan(attrDesc2.relName, status);
		if(status != OK) return status;

		// Restart the Index Scan for Next Loop
		status = index.startScan((char *) record1.data + attrDesc1.attrOffset);
		if(status != OK) return status;

		while(index.scanNext(rid2) == OK)
		{
			status = heap2.getRandomRecord(rid2, record2);
			if(status != OK) return status;

			char* data2 = (char *) record2.data;

			// Compare the values stored in both records
			int valueDif = matchRec(record1, record2, attrDesc1, attrDesc2);
			if(!valueDif)
			{
				// Add to output heap
				Record record;
				record.data = malloc(size);
				record.length = size;

				//Need to check through attrDescArray to find outside heap attrDescs
				for(int i = 0; i < projCnt; i++)
				{
					AttrDesc tempAttr = attrDescArray[i];

					//Insert from attrDesc1
					if( !strcmp(attrDesc1.relName, tempAttr.relName))
					{
						memcpy(((char*) record.data) + r[i].attrOffset, data1 + tempAttr.attrOffset, tempAttr.attrLen);
					} 
					else
					{
						memcpy(((char*) record.data) + r[i].attrOffset, data2 + tempAttr.attrOffset, tempAttr.attrLen);
					}
				}
				// Now insert the record into the output
				status = heapOut.insertRecord(record, rid);
				if(status != OK) return status;
			}
		}
		// End the scan and restart it again next iteration
		status = index.endScan();
		if(status != OK) return status;
	}

	status = heap1.endScan();
	if(status != OK) return status;

	return OK;
}

