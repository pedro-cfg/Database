package com.database;

import java.util.ArrayList;
import java.util.List;

public class UpdateOperation extends SQLOperation
{
    private String updateField;
    private String updateValue;
    private int fieldNumber;

    UpdateOperation(FileManager fm, String table, String field, String value, List<String> whereK)
    {
        super(fm);
        fromTable = table;
        whereKeys = whereK;
        updateField = field;
        updateValue = value;
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
        if(!getFieldNumber())
            return false;
        if(updateValue.contains(" "))
            updateValue = "\"" + updateValue + "\"";
        update();
        deleteAndConstruct();
        return true;
    }

    private boolean getFieldNumber()
    {
        boolean found = false;
        if(updateField.contains("."))
            updateField = updateField.split("\\.")[1];
        for(int i = 0; i < header.size(); i++)
        {
            if(header.get(i).value.equals(updateField))
            {
                found = true;
                fieldNumber = i;
            }
        }
        return found;
    }

    private void update()
    {
        List<String> line = new ArrayList<>();
        for(int i = 0; i < results.size(); i++)
        {
            for(int j = 0; j < finalResults.size(); j++)
            {
                if(finalResults.get(j).equals(results.get(i)))
                {
                    line = finalResults.get(j);
                    line.set(fieldNumber, updateValue);
                    finalResults.set(j, line);
                }
            }
        }
    }

}
