package com.database;

import java.util.List;

public class DeleteOperation extends SQLOperation
{
    DeleteOperation(FileManager fm, String table, List<String> whereK)
    {
        super(fm);
        fromTable = table;
        whereKeys = whereK;
    }

    public boolean execute()
    {
        if(!fileM.getHeader(header,fromTable))
            return false;
        if(!fileM.getResults(results, fromTable))
            return false;
        finalResults.addAll(results);
        if(!whereClause())
            return false;
        deleteFromResults();
        deleteAndConstruct();
        return true;
    }

    private void deleteFromResults()
    {
        for(int i = 0; i < results.size(); i++)
        {
            for(int j = 0; j < finalResults.size(); j++)
            {
                if(results.get(i).equals(finalResults.get(j)))
                    finalResults.remove(j);
            }
        }
    }
}
