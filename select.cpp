#include "catalog.h"
#include "query.h"
#include "index.h"

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used inthe selection predicate 
		         const Operator op,         // predicate operation
		         const void *attrValue)     // literal value in the predicate
{
	Status status = OK;

	// Compute recLen
	int reclen = 0;
	for(int i = 0; i < projCnt; i++)
	{
		reclen += projNames[i].attrLen;
	}

	// Create projNames attrDescs
	AttrDesc names[projCnt];
	for(int i = 0; i < projCnt; i++)
	{
		AttrDesc retAttr;
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, retAttr);
		if(status != OK) return status;

		names[i] = retAttr;
	}	

	// If a predicate exists
	if(attr)
	{
		AttrDesc attrDesc;

		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
		if(status != OK) return status;

		//If index exists on attribute in the predicate AND an equality, call IndexSelect
		if(op == EQ && attrDesc.indexed)
		{
			status = Operators::IndexSelect(result, projCnt, names, &attrDesc, op, attrValue, reclen);
			if(status != OK) return status;
		}
		else
		{
			status = Operators::ScanSelect(result, projCnt, names, &attrDesc, op, attrValue, reclen);
			if(status != OK) return status;
		}
	}
	else // predicate does NOT exist
	{
		status = Operators::ScanSelect(result, projCnt, names, NULL, op, attrValue, reclen);
	}

	return status;
}
