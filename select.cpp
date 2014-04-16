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
	if(attr) //if attr is NULL, predicate is unconditional
	{
		AttrDesc attrDesc;
		int reclen = 0;

		attrCat->getInfo(inString, attr->attrName, attrDesc);

		//Create projNames attrDescs
		AttrDesc names[projCnt];
		for(int i = 0; i < projCnt; i++)
		{
			AttrDesc attr;
			attrCat->getInfo(result, projNames[i].attrName, attr);
			names[i] = attr;
		}

		//If index exists on attribute in the predicate AND an equality, call IndexSelect
		if(op == EQ && attrDesc.indexed)
		{
			//Need to find attrDesc
			status = Operators::IndexSelect(result, projCnt, names, &attrDesc, op, attrValue, reclen);
		}
		else
		{
			//Need to find attrDesc and reclen
			status = Operators::ScanSelect(result, projCnt, names, &attrDesc, op, attrValue, reclen);
		}
	}

	return status;
}
