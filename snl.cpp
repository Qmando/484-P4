#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <stdio.h>
#include <string.h>

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
	cout << "Algorithm: Simple NL Join" << endl;
	
	RelDesc r;
	Status res3 = relCat->getInfo(result, r);
	cout << "Info " << res3 << " " << r.attrCnt << endl;
	
	Status res;
	Status res2;

	// Load heap 1
	HeapFileScan heap1 = HeapFileScan(attrDesc1.relName, res);
	if (res != OK) { return res; }
	
	// Load heap 2
	HeapFileScan heap2 = HeapFileScan(attrDesc2.relName, res);
	if (res != OK) { return res; }
	
	// Open output heap
	HeapFile output = HeapFile(result, res);
	if (res != OK) { return res; }
	
	// Describe output relation attrs
	int size=0;
	for (int x=0;x<projCnt;x++) {
		AttrDesc newAttr;
		newAttr.attrLen = attrDescArray[x].attrLen;
		newAttr.attrOffset = size;
		newAttr.attrType = attrDescArray[x].attrLen;
		memcpy(newAttr.attrName, attrDescArray[x].attrName, MAXNAME);
		memcpy(newAttr.relName, &result, MAXNAME);
		newAttr.indexed = 0;
		size += attrDescArray[x].attrLen;
		//res = attrCat->addInfo(newAttr);
	}
	
	// Output relation description
	
	
	
	// Start scan
	RID rid1;
	RID rid2;
	Record rec1;
	Record rec2;
  	res = heap1.startScan(attrDesc1.attrOffset, attrDesc1.attrLen, (Datatype)attrDesc1.attrType, NULL, EQ);
	if (res != OK) { return res; }
	
	while (res) { // Outer loop
		res = heap1.scanNext(rid1);
		if (res != OK) { break; }
		res = heap1.getRecord(rid1, rec1);
		if (res != OK) { break; }
		void* data1 = rec1.data;
		int data1len = rec1.length;
		
		
		res2 = heap2.startScan(attrDesc2.attrOffset, attrDesc2.attrLen, (Datatype)attrDesc2.attrType, NULL, EQ);
		while (res2) { // Inner loop
			res2 = heap2.scanNext(rid2);
			if (res != OK) { break; }
			res = heap2.getRecord(rid2, rec2);
			void* data2 = rec2.data;
			int data2len = rec2.length;
			
			if ((op == EQ && memcmp(data1, data2, min(data1len, data2len)) == 0) ||
				(op == LT && memcmp(data1, data2, min(data1len, data2len)) < 0) ||
				(op == LTE && memcmp(data1, data2, min(data1len, data2len)) <= 0) ||
				(op == GT && memcmp(data1, data2, min(data1len, data2len)) > 0) ||
				(op == GTE && memcmp(data1, data2, min(data1len, data2len)) >= 0) ||
				(op == NE && memcmp(data1, data2, min(data1len, data2len)) != 0)) {
				
				// Add to output heap
				Record record;
				for (int x=0;x<projCnt;x++) {
					AttrDesc attr = attrDescArray[x];
				}
					
				
				
			}
		}
	}
		
  	return OK;
}

