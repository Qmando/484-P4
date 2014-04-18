#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <stdlib.h>
#include <stdio.h>
#include <cstring>
#include <set>
#include "utility.h"

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
	cout << "Algorithm: Simple NL Join" << endl;
	
	AttrDesc* r;
	Status status;
	int num;

	// Get Relation information for the Output relation
	status = attrCat->getRelInfo(result, num, r);
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
	int size=0;
	for (int x=0;x<projCnt;x++) {
		size += attrDescArray[x].attrLen;
	}


	RID rid, rid1, rid2;
	Record rec1, rec2;
	
	// Start scan to fill output heap
	while(heap1.scanNext(rid1, rec1) == OK)
	{
		// Retrieve record 1's data and reset heap 2 filescan
		char* data1 = (char*)rec1.data;
		heap2 = HeapFileScan(attrDesc2.relName, status);
		if(status != OK) return status;

		//Compare the data from record 1 with ALL of the records from heap 2 filescan
		while(heap2.scanNext(rid2, rec2) == OK )
		{
			char* data2 = (char*)rec2.data;
			//int compRes =  memcmp(data1+attrDesc1.attrOffset, 
			//			data2+attrDesc2.attrOffset, 
			//			min(attrDesc1.attrLen, attrDesc2.attrLen));					
			int compRes = matchRec(rec1, rec2, attrDesc1, attrDesc2);

			if ((op == EQ && compRes == 0) ||
				(op == LT && compRes < 0) ||
				(op == LTE && compRes <= 0) ||
				(op == GT && compRes > 0) ||
				(op == GTE && compRes >= 0) ||
				(op == NE && compRes != 0)) 
			{
				// Add to output heap
				Record record;
				record.data = malloc(size);
				record.length = size;

				for(int i = 0; i < projCnt; i++) {
					AttrDesc tempAttr = attrDescArray[i];

					// Insert from attrDesc1
					if( !strcmp(attrDesc1.relName, tempAttr.relName))
					{
						memcpy(((char*) record.data) + r[i].attrOffset, data1 + tempAttr.attrOffset, tempAttr.attrLen);
					} 
					else // Insert from attrDesc2
					{
						memcpy(((char*) record.data) + r[i].attrOffset, data2 + tempAttr.attrOffset, tempAttr.attrLen);
					}
				}
				// Now insert the record into the output
				status = heapOut.insertRecord(record, rid);	
				if(status != OK) return status;
			}
		}
	}
  	return OK;
}

