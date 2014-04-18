#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
#include <stdlib.h>

Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
	cout << "Algorithm: Index Select" << endl;

	// Create/Find the heapfile with the name of the relation
	Status status;
	HeapFileScan heapIn = HeapFileScan(attrDesc->relName, status);
	if(status != OK) return status;

	HeapFile heapOut = HeapFile(result, status);
	if(status != OK) return status;

	// Create/find the index (checked to ensure indexed before entering this fn)
	Index index = Index(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, 1, status);
    	if (status != OK) return status;

	// Start the index scan
	status = index.startScan(attrValue);
	if(status != OK) return status;	

	// Reset size for the new relation and update the length
	AttrDesc *r;
	int len = 0, updatedLength = 0;

	status = attrCat->getRelInfo(result, len, r);
	if(status != OK) return status;

	for(int i = 0; i < len; i++)
	{
		updatedLength += projNames[i].attrLen;
	}

	// Go through index and store records into the results heap page
	RID rid;
	Record record, newRecord;
	while(index.scanNext(rid) != NOMORERECS)
	{
		// Return a reference to record with rid
		status = heapIn.getRandomRecord(rid, record);
		if(status != OK) return status;

		// Copy memory into new Record
		newRecord.data = malloc(updatedLength);
	
		for(int i = 0; i < len; i++)
		{
			memcpy((char *) newRecord.data + r[i].attrOffset , 
				(char *) record.data + projNames[i].attrOffset , r[i].attrLen);
		}

		newRecord.length = updatedLength;

		// Store the new Record in the heap page
		status = heapOut.insertRecord(newRecord, rid); 
		if(status != OK) return status;
	}
	status = index.endScan();

  	return status;
}

