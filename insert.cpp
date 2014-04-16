#include "catalog.h"
#include "query.h"
#include "index.h"

/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    // Get the info for this relation
    RelDesc description;
    Status res = getInfo(relation, description);
    if (res != OK) {
    	return res;
    }
    
    // Get the information about the attributes
    int attrCntInfo;
    AttrDesc*[] attrs; 
    res = attrCat->getRelInfo(relation, attrCntInfo, attrs);
    if (res != OK) {
    	return res;
    }
    
    // Compute record size
    AttrDesc* curAttr;
    int size = 0;
    for (int x=0;x<attrCntInfo;x++) {
    	curAttr = attrs[x];
    	size += curAttr->attrLen;
    }

    // Allocate record
    Record record;
    record.length = size;
    record.data = malloc(size);
    
    attrInfo curInfo;
    for (int x=0;x<attrCnt;x++) {
    	curInfo = attrList[x];
    	
    	// Get the description of this attribute
    	AttrDesc desc;
    	res = attrCat->getInfo(relation, curInfo.attrName, desc);
    	if (res != OK) {
    		return res;
    	}
    	
    	// Copy the data into the record at the correct offset
		memcpy(record.data[desc.attrOffset], curInfo.attrValue, desc.attrLen);
	}
    		
    // Find the heapfile and write the record
    RID rid;
    HeapFile heap = HeapFile("heap", res);
    if (res != OK) {
    	return res;
    }
    heap.insertRecord(record, rid);	
    	
	// Find which attrs are indexed
	for (int x=0;x<attrCnt;x++) {
    	curInfo = attrList[x];
    	
    	// Get the description of this attribute
    	AttrDesc desc;
    	res = attrCat->getInfo(relation, curInfo.attrName, desc);
    	if (res != OK) {
    		return res;
    	}
    	
    	// Create/find the index and insert the RID
    	if (desc.indexed) {
    		Index index = Index(curInfo->attrName, desc->attrOffset, desc->attrLen, desc->attrType, 1, res);
    		if (res != OK){
    			return res;
    		}
    		index.insertEntry(curInfo.attrValue, RID);
    	}
	}
	
    return OK;
}
