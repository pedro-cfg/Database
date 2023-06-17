package com.database;

import java.util.ArrayList;
import java.util.List;

public class InsertOperation extends SQLOperation
{
    private List<String> insertFields;
    private List<String> insertValues;

    InsertOperation(FileManager fm, List<String> fields, List<String> values, String table)
    {
        super(fm);
        insertFields = fields;
        insertValues = values;
        fromTable = table;
    }

    public boolean execute()
    {
        return write();
    }

    private boolean verifyExistance(String insert)
    {
        String[] words = insert.split(",");
        boolean exists = false;
        List<List<String>> results = new ArrayList<>();
        if(!fileM.getResults(results, fromTable))
            return false;
        List<String> line = new ArrayList<>();
        for(int i = 0; i < results.size(); i++)
        {
            line = results.get(i);
            boolean equalLine = true;
            for(int j = 0; j < line.size(); j++)
            {
                if(!line.get(j).equals(words[j]))
                    equalLine = false;
            }
            if(equalLine)
                exists = true;
        }
        return exists;
    }

    private boolean verifyTables()
    {
        boolean verified = true;
        for(int i = 0; i < insertFields.size(); i++)
        {
            boolean exists = false;
            for(int j = 0; j < header.size(); j++)
            {
                if(header.get(j).value.equals(insertFields.get(i)))
                    exists = true;
            }
            if(!exists)
                verified = false;
        }
        return verified;
    }

    private boolean write()
    {
        List<String> newLine = new ArrayList<>();
        if(!fileM.getHeader(header,fromTable))
            return false;
        if(!verifyTables())
            return false;
        boolean exists = false;
        for(int i = 0; i < header.size(); i++)
        {
            if(insertFields.size() == 0)
            {
                if(i < insertValues.size())
                {
                    if(insertValues.get(i).contains(" "))
                        newLine.add("\"" + insertValues.get(i) + "\"");
                    else
                        newLine.add(insertValues.get(i));
                }
                else
                    return false;
            }
            else
            {
                exists = false;
                for(int j = 0; j < insertFields.size(); j++)
                {
                    if(header.get(i).value.equals(insertFields.get(j)))
                    {
                        exists = true;
                        if(insertValues.get(j).contains(" "))
                            newLine.add("\"" + insertValues.get(j) + "\"");
                        else
                            newLine.add(insertValues.get(j));
                    }
                }
                if(!exists)
                {
                    newLine.add("NULL");
                }
            }
        }
        String formattedLine = constructFormattedLine(newLine);
        if(verifyExistance(formattedLine))
            return false;
        return fileM.appendLine(fromTable, formattedLine);
    }
}
