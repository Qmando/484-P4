#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <vector>
#include <cstring>
#include <stdlib.h>
#include <stdio.h>

/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
    cout << "Algorithm: SM Join" << endl;
    Status res;
    
    AttrDesc* r;
    int num;
    int size1=0;
    int size2=0;
    res = attrCat->getRelInfo(result, num, r);
    
    AttrDesc* r1;
    res = attrCat->getRelInfo(attrDesc1.relName, num, r1);
    for (int x=0;x<num;x++) {
    	size1 += r1[x].attrLen;
    }
    
    AttrDesc* r2;
    res = attrCat->getRelInfo(attrDesc2.relName, num, r2);
    for (int x=0;x<num;x++) {
    	size2 += r2[x].attrLen;
    }
    
  
    
    // Find useable buffer pages
    int numPages = bufMgr->numUnpinnedPages();
    int k = (int) ((float)(numPages*.8));
    
    // Get size of output record
    int size=0;
    for (int x=0;x<projCnt;x++) {
        size += attrDescArray[x].attrLen;
    }
    
   
    int maxTuples1 = k/2 * 1024 / size1;
    int maxTuples2 = k/2 * 1024 / size2;
    int numTuples1 = HeapFile(attrDesc1.relName, res).getRecCnt();
    int numTuples2 = HeapFile(attrDesc2.relName, res).getRecCnt();
    
    Record rec1;
    Record rec2;
    
    // Open sorted files
    SortedFile file1 = SortedFile(attrDesc1.relName, attrDesc1.attrOffset, 
                            attrDesc1.attrLen, (Datatype)attrDesc1.attrType,
                            maxTuples1, res);
    if (res != OK) { return res; }
    SortedFile file2 = SortedFile(attrDesc2.relName, attrDesc2.attrOffset, 
                            attrDesc2.attrLen, (Datatype)attrDesc2.attrType,
                            maxTuples2, res);
    if (res != OK) { return res; }
    
    // Open output heap
    HeapFile output = HeapFile(result, res);
    if (res != OK) { cout << "out heap err"; return res; }
    
    res = file1.next(rec1);
    if (res != OK) { Error::print(res); return res; }
    res = file2.next(rec2);
    if (res != OK) { Error::print(res); return res; }
    
    
    numTuples1--;
    numTuples2--;
    char* data1 = (char*)rec1.data;
    char* data2 = (char*)rec2.data;
    char* curData = data1;
    Record curRec = rec1;
    bool break_again = false;
    
    std::vector<Record> relSubset1;
    std::vector<Record> relSubset2;
    
        
    while (1) {
        relSubset1.clear();
        relSubset2.clear();
        
        //cout << "Rec1 " << *((int*)(data1+attrDesc1.attrOffset)) << " Rec2 " << *((int*)(data2+attrDesc2.attrOffset)) << endl;
        curData=data1;
        curRec = rec1;
        
        // Advance rec1 until its >= to rec2
        while (Operators::matchRec(rec1, rec2, attrDesc1, attrDesc2) < 0) {
            //cout << "Assert " << *((int*)(data1+attrDesc1.attrOffset)) << " < " << *((int*)(data2+attrDesc2.attrOffset)) << endl;
            if (file1.next(rec1) != OK) { return OK; }
            data1 = (char*)rec1.data;
            curData = data1;
            curRec = rec1;
            
        }
        //cout << "CurRec, rec1 == to " << *((int*)(data1+attrDesc1.attrOffset)) << endl;
            
        // Start the subset
        relSubset1.push_back(rec1);
            
            // Insert rel1 values into relsubset until it differs
            while (1) { 
                if (file1.next(rec1) != OK) { break; }
                data1 = (char*)rec1.data;
                
                if (Operators::matchRec(rec1, curRec, attrDesc1, attrDesc1) != 0) {
                    break;
                }
                //cout << "Adding to 1" << endl;
                relSubset1.push_back(rec1);
            }
            
        // Advance rec2 until its == to rec1
        while (Operators::matchRec(curRec, rec2, attrDesc1, attrDesc2) > 0) {
            if (file2.next(rec2) != OK) { return OK; }
            data2 = (char*)rec2.data;
            //cout << "Advancing 2 to " << *((int*)(data2+attrDesc2.attrOffset)) << endl;
            
            // If we went too far, back to the beginning!
            if (Operators::matchRec(curRec, rec2, attrDesc1, attrDesc2) < 0) {
                relSubset1.clear();
                relSubset2.clear();
                break_again = true;
                //cout << "Went too far!" << endl;
                break;
            }
        }
        
        if (break_again) {
        	break_again = false;
        	continue;
        }
            
        // We can assert here that data2 == relSubSet1 data
        //cout << "Assert " << *((int*)(curData+attrDesc1.attrOffset)) << " == " << *((int*)(data2+attrDesc2.attrOffset)) << endl;
          
            // Insert rel2 values into relsubset2 until it's higher and differs
            while (1) {
                relSubset2.push_back(rec2);
                //cout << "Adding to subset2" << endl;
                if (file2.next(rec2) != OK) { break; }
                data2 = (char*)rec2.data;
                
                if (Operators::matchRec(rec2, curRec, attrDesc2, attrDesc1) != 0) {
                    //cout << "Advanced to " << *((int*)(data2+attrDesc2.attrOffset)) << ".. not equal anymore!" << endl;
                    break;
                } 
            }
            
            // Set curData to new data
            curData = data1;
            curRec = rec1;
            
            // Output cross product of subsets 
            for(vector<Record>::iterator recIt1=relSubset1.begin();recIt1!=relSubset1.end();++recIt1) {
                for(vector<Record>::iterator recIt2=relSubset2.begin();recIt2!=relSubset2.end();++recIt2) {
                
                    Record record;
                    record.data = malloc(size);
                    record.length = size;
                    
                    // Copy all projected attrs into output
                    for (int x=0;x<projCnt;x++) {
                        AttrDesc outAttr = attrDescArray[x];
                        if (strcmp(outAttr.relName, attrDesc1.relName) == 0){
                            memcpy(((char*)record.data)+r[x].attrOffset,
                                    ((char*)(*recIt1).data)+outAttr.attrOffset,
                                    outAttr.attrLen);
                        }
                        else {
                            memcpy(((char*)record.data)+r[x].attrOffset,
                                    ((char*)(*recIt2).data)+outAttr.attrOffset,
                                    outAttr.attrLen);
                        }
                    }
                    RID rid;
                    output.insertRecord(record, rid);    
                }
            }
        
    }    
    return OK;
}


