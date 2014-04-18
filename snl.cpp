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
	int num;
	Status res3 = attrCat->getRelInfo(result, num, r);
	
	cout << "Attr1 " << attrDesc1.attrName << " at offset " << attrDesc1.attrOffset << endl;
	cout << "Attr2 " << attrDesc2.attrName << " at offset " << attrDesc2.attrOffset << endl;
	Status res;
	Status res2;

	// Load heap 1
	HeapFileScan heap1 = HeapFileScan(attrDesc1.relName, res);
	if (res != OK) { cout << "heap1 err";return res; }
	
	// Load heap 2
	HeapFileScan heap2 = HeapFileScan(attrDesc2.relName, res);
	if (res != OK) { cout << "heap2 err";return res; }
	
	// Open output heap
	HeapFile output = HeapFile(result, res);
	if (res != OK) { cout << "out heap err";return res; }
	
	// Get size of output record
	int size=0;
	for (int x=0;x<projCnt;x++) {
		size += attrDescArray[x].attrLen;
	}
	
	// Start scan
	RID rid1;
	RID rid2;
	RID rid;
	Record rec1;
	Record rec2;
	
	while (res == OK) { // Outer loop
		res = heap1.scanNext(rid1, rec1);
		if (res != OK) { break; }
		char* data1 = (char*)rec1.data;
		
		heap2 = HeapFileScan(attrDesc2.relName, res2);
		while (res2 == OK) { // Inner loop
			res2 = heap2.scanNext(rid2, rec2);
			if (res2 != OK) { break; }
			char* data2 = (char*)rec2.data;
			
			int compRes =  memcmp(data1+attrDesc1.attrOffset, 
									data2+attrDesc2.attrOffset, 
									min(attrDesc1.attrLen, attrDesc2.attrLen));
			
			if ((op == EQ && compRes == 0) ||
				(op == LT && compRes < 0) ||
				(op == LTE && compRes <= 0) ||
				(op == GT && compRes > 0) ||
				(op == GTE && compRes >= 0) ||
				(op == NE && compRes != 0)) {
				
				// Add to output heap
				Record record;
				record.data = malloc(size);
				record.length = size;
				
				bool rel1 = true;
				std::set<int> offsets;
				for (int x=0;x<projCnt;x++) {
					AttrDesc outAttr = attrDescArray[x];
					AttrDesc inAttr;
					// get all attrs from rel1
					if (rel1) {
						if (attrCat->getInfo(attrDesc1.relName, outAttr.attrName, inAttr) == OK) {
							// Once we hit the same offset twice, we are now reading from relation 2
							if (offsets.count(inAttr.attrOffset) > 0) {rel1 = false; }
							else {
								offsets.insert(inAttr.attrOffset);
								memcpy(((char*)record.data)+r[x].attrOffset, 
										data1+inAttr.attrOffset, 
										inAttr.attrLen);
							}
						}
						else { rel1 = false; } // We hit the end of rel1 projs
					}
					if (!rel1 && attrCat->getInfo(attrDesc2.relName, outAttr.attrName, inAttr) == OK) {
						memcpy(((char*)record.data)+r[x].attrOffset, 
								data2+inAttr.attrOffset, 
								inAttr.attrLen);
					}
				}
				
				output.insertRecord(record, rid);	
				
				
			}
		}
	}	
  	return OK;
}

